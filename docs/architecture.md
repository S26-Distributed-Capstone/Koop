# System Architecture

## Overview

KoopDB is a distributed object storage system designed for fault tolerance and scalability. It splits data into erasure-coded shards and distributes them across a cluster of independent storage nodes. No single node holds the complete file, yet the file can be retrieved even if multiple nodes fail.

### Core Components

The architecture consists of three primary services:

1.  **Query Processors (Gateway):** 
    - **Stateless Front-End:** Accepts S3 API requests from clients.
    - **Erasure Coding:** Encodes incoming data into shards (k + m) and distributes them to storage nodes. Reconstructs data from shards on retrieval.
    - **Routing:** Uses consistent hashing or partition mapping (via Etcd) to determine which storage nodes hold the data for a given object key.
    - **Multipart Manager:** Coordinates multipart uploads, tracking parts and assembling the final object metadata.

2.  **Storage Nodes:**
    - **Stateful Backend:** Stores the actual data shards and object metadata.
    - **Storage Engine:** Uses **RocksDB** (embedded key-value store) for persistence.
    - **Operation Log:** Maintains a log of operations (PUT, DELETE) per partition for consistency and repair.
    - **Repair Mechanisms:** Background processes to detect missing or corrupted shards and repair them from peers.

3.  **Coordination Cluster (Metadata & State):**
    - **Etcd:** Stores the cluster topology (node membership, erasure set configuration, partition mapping). Acts as the single source of truth for configuration updates.
    - **Redis:** Used by Query Processors to coordinate multipart upload state (session management) and potentially for distributed locking/sequencing.

### Data Flow

#### 1. PUT Object
1.  **Client** sends a `PUT /bucket/key` request to the Load Balancer (or any Query Processor).
2.  **QP** receives the data stream.
3.  **QP** chunks the stream and applies **Erasure Coding** (e.g., 6 data shards + 3 parity shards).
4.  **QP** determines the correct storage nodes for the object's partition (based on Etcd config).
5.  **QP** writes shards in parallel to the 9 target **Storage Nodes**.
6.  **Storage Nodes** write the shard metadata and data to **RocksDB** in two-phased commit - write data and then commit the message using kafka as an atomic broadcast.
7.  **QP** waits for a write quorum (e.g., 6/9) before acknowledging success to the client.

#### 2. GET Object
1.  **Client** sends a `GET /bucket/key` request.
2.  **QP** determines the correct storage nodes.
3.  **QP** requests shards from the relevant nodes and reconciles version descrepancies.
4.  **QP** reconstructs the original object from any `k` shards (e.g., 6) using Reed-Solomon decoding.
5.  **QP** streams the data back to the client.

#### 3. Metadata Updates
- Topology changes (adding/removing nodes) are updated in **Etcd**.
- **Storage Workers** (inside QPs) and **Storage Nodes** watch Etcd keys for changes.
- Upon an update event, all components refresh their internal routing tables to reflect the new cluster state.

## Diagram Reference

For a visual representation of these components and their interactions, see the [C4 Container Diagram](diagrams/workspace.dsl) and the rendered SVG diagrams in the `docs/diagrams/` folder.
