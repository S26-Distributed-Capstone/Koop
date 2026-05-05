# System Architecture

## Overview

KoopDB is a distributed object storage system designed for fault tolerance and scalability. It splits data into erasure-coded shards and distributes them across a cluster of independent storage nodes. No single node holds the complete file, yet the file can be retrieved even if multiple nodes fail.

### Core Components

The architecture consists of four primary services:

1.  **Query Processors (Gateway):**
    - **Stateless Front-End:** Accepts S3 API requests from clients.
    - **Erasure Coding:** Encodes incoming data into `k` data + `(n-k)` parity shards and distributes them to storage nodes. Reconstructs data from any `k` available shards on retrieval.
    - **Routing:** Maps each `(bucket, key)` to a partition and to an erasure set using configuration loaded from Etcd (`erasure_set_configurations`, `partition_spread_configurations`).
    - **Multipart Manager:** Coordinates multipart upload sessions, tracking parts and publishing the final commit message.

2.  **Storage Nodes:**
    - **Stateful Backend:** Receives erasure-coded shards over HTTP and persists them to disk.
    - **Storage Engine:** Uses **RocksDB** (embedded key-value store) for the OpLog, object metadata, and bucket tables.
    - **Operation Log:** Maintains a per-partition, sequenced log of mutations (PUT, DELETE, bucket ops) used for repair.
    - **Repair Worker Pool:** Background workers (`RepairWorkerPool` in `StorageNodeServerV2`) detect missed sequence numbers when a node comes back online and replay them from peers to catch up.

3.  **Kafka (Sequencer / Pub-Sub):**
    - Provides total ordering of mutating operations on a per-partition basis.
    - Query Processors publish ordered commit messages (`PutMessage`, `DeleteMessage`, `CreateBucketMessage`, `DeleteBucketMessage`, `MultipartCommitMessage`) to partition-keyed topics.
    - Storage nodes consume those messages and apply the corresponding atomic RocksDB writes.

4.  **Coordination Cluster (Metadata & State):**
    - **Etcd (3-node quorum):** Stores cluster topology, erasure-set configuration, and partition→erasure-set mapping. Acts as the source of truth for routing config; Query Processors and Storage Workers watch the relevant keys for changes.
    - **Redis:** Used by Query Processors to track multipart upload session state (active sessions, uploaded parts, cached part sizes). A `MemoryCacheClient` is available as an in-process substitute for dev/test.

### Data Flow

#### 1. PUT Object

The system uses a two-stage write: shards are streamed first, and only after a write quorum acknowledges the shard upload is an ordered commit message published. This is **not** XA-style two-phase commit — there is no prepare/abort vote — but it does separate durable shard placement from the ordered metadata commit.

1.  **Client** sends a `PUT /{bucket}/{key}` request to any Query Processor.
2.  **QP** receives the data stream and erasure-encodes it into `n` shards (default `n=6`, `k=4`).
3.  **QP** looks up the partition's erasure set in Etcd and streams shards to the target storage nodes concurrently over HTTP (`PUT /store/{partition}/{storageKey}`).
4.  **QP** waits for at least `write_quorum` shard upload ACKs from the storage nodes.
5.  **QP** publishes an ordered `PutMessage` via Kafka to the partition's topic.
6.  **Storage Nodes** consume the sequenced commit message and atomically write OpLog + Metadata in RocksDB.
7.  **QP** waits for `write_quorum` commit ACKs and then responds `200 OK` to the client.

#### 2. GET Object

1.  **Client** sends a `GET /{bucket}/{key}` request.
2.  **QP** resolves the erasure set for the key and queries all `n` storage nodes for their shard.
3.  **QP** reconciles version conflicts: the version that a read quorum agrees on is selected. If no version reaches quorum the request fails with `500 InternalError`.
4.  **QP** reconstructs the original object from any `k` available shards via Reed-Solomon decoding.
5.  **QP** streams the data back to the client.

#### 3. DELETE / Bucket Ops

DELETE, CreateBucket, and DeleteBucket follow the same Kafka-sequenced pattern as PUT: the QP publishes an ordered message, storage nodes consume it and apply an atomic RocksDB write (tombstone for deletes), and the client is acknowledged once a write quorum confirms the commit.

#### 4. Topology / Configuration Updates

- Topology and erasure-set changes are written to **Etcd**.
- Query Processors (and storage nodes where applicable) watch Etcd keys for changes and refresh their internal routing tables on update.

#### 5. Repair

When a storage node restarts and detects a gap in its sequence numbers, its `RepairWorkerPool` requests the missing operations from peer nodes and replays them, skipping any keys whose post-recovery state already supersedes the replayed op. See [`workflow.md`](workflow.md#12-storage-node-repair-flow) for the full flow.

## Diagram Reference

For a visual representation of these components and their interactions, see the [C4 Container Diagram](diagrams/workspace.dsl) and the rendered SVG diagrams in the `docs/diagrams/` folder.
