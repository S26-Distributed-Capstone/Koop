# Workflow Diagrams

This document displays workflow diagrams for each supported use case in the Koop distributed object store. Koop exposes an S3-compatible API; all operations flow through the **Query Processor (QP)** gateway before being fanned out to **Storage Nodes (SN)** via erasure-coded shards.

> **Related documents:**
> - [Scope](scope.md) — supported use cases and error cases
> - [Architecture](architecture.md) — system component overview and redundancy model

---

## Table of Contents

1. [System Component Overview](#system-component-overview)
2. [PUT Object](#1-put-object)
3. [GET Object](#2-get-object)
4. [DELETE Object](#3-delete-object)
5. [Create Bucket](#4-create-bucket)
6. [Delete Bucket](#5-delete-bucket)
7. [Head Bucket (Bucket Exists)](#6-head-bucket-bucket-exists)
8. [List Objects in Bucket](#7-list-objects-in-bucket)
9. [Multipart Upload — Create](#8-multipart-upload--create)
10. [Multipart Upload — Upload Part](#9-multipart-upload--upload-part)
11. [Multipart Upload — Complete](#10-multipart-upload--complete)
12. [Multipart Upload — Abort](#11-multipart-upload--abort)
13. [Storage Node Repair Flow](#12-storage-node-repair-flow)
14. [Gossip-Based Garbage Collection](#13-gossip-based-garbage-collection)

---

## System Component Overview

The following diagram shows the high-level components involved in every workflow. Each request enters through the S3-compatible HTTP gateway ([`Main.java`](../query-processor/src/main/java/com/github/koop/queryprocessor/gateway/Main.java)), is processed by the [`StorageWorker`](../query-processor/src/main/java/com/github/koop/queryprocessor/processor/StorageWorker.java), and is fanned out to 9 storage nodes per erasure set. Kafka acts as the global sequencer for write/delete ordering, delivering an ordered operation log to every storage node. Redis is used separately by the Query Processor for multipart upload session tracking. Each storage node persists shard data to disk and records metadata + operation logs atomically in RocksDB via the [`Database`](../storage-node/src/main/java/com/github/koop/storagenode/db/Database.java) layer.

```mermaid
graph TD
    Client["S3 Client"]
    QP["Query Processor\n(Javalin Gateway + StorageWorker)"]
    Kafka["Kafka\n(Write/Delete Sequencer)"]
    Redis["Redis\n(Multipart Upload Cache)"]
    Etcd["etcd\n(Partition & Erasure Set Config)"]
    SN1["Storage Node 1"]
    SN2["Storage Node 2"]
    SN3["Storage Node 3 … 9"]
    RocksDB["RocksDB\n(OpLog + Metadata + Buckets)"]
    Disk["Disk\n(Shard data.dat)"]

    Client -->|"S3 HTTP"| QP
    QP -->|"reads config"| Etcd
    QP -->|"publish operation"| Kafka
    QP -->|"multipart session state"| Redis
    QP -->|"erasure-coded shards"| SN1
    QP -->|"erasure-coded shards"| SN2
    QP -->|"erasure-coded shards"| SN3
    SN1 --> RocksDB
    SN1 --> Disk
    SN2 --> RocksDB
    SN2 --> Disk
    SN3 --> RocksDB
    SN3 --> Disk
    Kafka -->|"consume ordered ops"| SN1
    Kafka -->|"consume ordered ops"| SN2
    Kafka -->|"consume ordered ops"| SN3
```

---

## 1. PUT Object

**Route:** `PUT /{bucket}/{key}`

The QP erasure-encodes the object into 9 shards and streams them concurrently to all storage nodes. The QP then publishes the PUT operation to Kafka, which assigns a sequence number and delivers it to every storage node in order. Each node atomically commits the operation log and metadata in RocksDB. A write quorum of ≥ 6 out of 9 nodes must acknowledge before the client receives a response.

See [`StorageWorker.put()`](../query-processor/src/main/java/com/github/koop/queryprocessor/processor/StorageWorker.java:65) and [`StorageNode.store()`](../storage-node/src/main/java/com/github/koop/storagenode/StorageNode.java:122).

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Kafka as Kafka (Sequencer)
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)
    participant Disk as Disk (each SN)

    C->>QP: PUT /{bucket}/{key} [data]
    QP->>QP: Erasure-encode → 9 shards
    par Stream shards concurrently
        QP->>SN: PUT /store/{partition}/{key}?requestId=X [shard i]
    end
    SN-->>QP: ACK (shard written to disk)
    Note over QP: Wait for ≥6 ACKs (write quorum)

    QP->>Kafka: Publish PUT operation for key
    Kafka->>SN: Deliver ordered operation to all nodes (consumer group)

    alt Node has shard data
        SN->>RDB: Atomic write → OpLog + Metadata
    else Node missed data stream
        SN->>SN: Reconstruct shard from peers
        SN->>RDB: Atomic write → OpLog + Metadata
    end

    SN-->>QP: ACK (metadata committed)
    Note over QP: Wait for ≥6 ACKs
    QP-->>C: 200 OK
```

### Error Cases

```mermaid
flowchart TD
    A[PUT request received] --> B{Content-Length > 0?}
    B -- No --> Z1[400 Bad Request]
    B -- Yes --> C[Erasure-encode + stream shards]
    C --> D{≥6 shard ACKs received?}
    D -- No --> E[Retry remaining nodes]
    E --> F{Retry succeeded?}
    F -- No --> Z2[500 Internal Error to client]
    F -- Yes --> G[Publish to Kafka]
    D -- Yes --> G
    G --> H{Kafka publish ACK received?}
    H -- No --> I[Retry publish to Kafka]
    I --> G
    H -- Yes --> J{≥6 metadata ACKs?}
    J -- No --> Z2
    J -- Yes --> K[200 OK to client]
```

---

## 2. GET Object

**Route:** `GET /{bucket}/{key}`

The QP queries all 9 storage nodes for their shard. At least 6 shards (the erasure coding threshold `K`) must be available to reconstruct the object. The QP streams the reconstructed data back to the client.

See [`StorageWorker.get()`](../query-processor/src/main/java/com/github/koop/queryprocessor/processor/StorageWorker.java:143) and [`StorageNode.retrieve()`](../storage-node/src/main/java/com/github/koop/storagenode/StorageNode.java:171).

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant SN as Storage Nodes (×9)
    participant Disk as Disk (each SN)

    C->>QP: GET /{bucket}/{key}
    QP->>QP: Hash key → erasure set + partition
    par Request shards concurrently
        QP->>SN: GET /store/{partition}/{key}
        SN->>Disk: Read shard data.dat (via "current" version pointer)
        Disk-->>SN: shard bytes
        SN-->>QP: 200 + shard bytes
    end
    Note over QP: Collect available shards
    alt ≥6 shards available
        QP->>QP: Erasure-decode → reconstruct object
        QP-->>C: 200 OK [object data stream]
    else <6 shards available
        QP-->>C: 500 Internal Error (too many nodes lost)
    end
```

### Conflicting Versions

When nodes return different versions of the same key (e.g., a write is mid-commit), the QP returns the version that at least a read quorum (6/9) of nodes agree on. If no version reaches quorum, the operation fails immediately — the system does **not** wait for stabilization.

```mermaid
flowchart TD
    A[Collect shard responses] --> B{All nodes agree on version?}
    B -- Yes --> C[Erasure-decode + return to client]
    B -- No --> D{Does a read quorum ≥6 share the same version?}
    D -- Yes --> E[Use quorum version → decode + return]
    D -- No --> F[500 Internal Error\nNo quorum version available]
```

---

## 3. DELETE Object

**Route:** `DELETE /{bucket}/{key}`

DELETE is sequenced through Kafka before storage nodes apply a tombstone to their metadata and operation log. No shard data is immediately removed from disk; physical deletion is handled asynchronously.

See [`StorageWorker.delete()`](../query-processor/src/main/java/com/github/koop/queryprocessor/processor/StorageWorker.java:175) and [`StorageNode.delete()`](../storage-node/src/main/java/com/github/koop/storagenode/StorageNode.java:187).

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Kafka as Kafka (Sequencer)
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)

    C->>QP: DELETE /{bucket}/{key}
    QP->>Kafka: Publish DELETE operation for key
    Kafka->>SN: Deliver ordered operation to all nodes
    SN->>RDB: Atomic write → Tombstone in Metadata + OpLog entry
    SN-->>QP: ACK
    Note over QP: Wait for ≥6 ACKs
    QP-->>C: 204 No Content

    Note over SN: Physical shard deletion is async (background thread)
```

---

## 4. Create Bucket

**Route:** `PUT /{bucket}`

Bucket creation is sequenced through Kafka. Storage nodes store the bucket record in their RocksDB bucket table. A write quorum must acknowledge before the client is notified.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Kafka as Kafka (Sequencer)
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)

    C->>QP: PUT /{bucket}
    QP->>QP: Check if bucket already exists
    alt Bucket already exists
        QP-->>C: 409 Conflict (S3 BucketAlreadyExists)
    else Bucket does not exist
        QP->>Kafka: Publish CREATE_BUCKET operation
        Kafka->>SN: Deliver ordered operation to all nodes
        SN->>RDB: Atomic write → Bucket table + OpLog entry
        SN-->>QP: ACK
        Note over QP: Wait for write quorum ACKs
        QP-->>C: 200 OK
    end
```

---

## 5. Delete Bucket

**Route:** `DELETE /{bucket}`

Bucket deletion writes a tombstone to the bucket table on each storage node. The bucket record is logically deleted; any remaining objects in the bucket are not immediately purged.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)

    C->>QP: DELETE /{bucket}
    QP->>SN: Send delete_bucket command to all nodes
    SN->>RDB: Write tombstone → Bucket table + OpLog entry
    SN-->>QP: ACK
    QP-->>C: 204 No Content
```

---

## 6. Head Bucket (Bucket Exists)

**Route:** `HEAD /{bucket}`

A lightweight existence check. The QP queries the bucket table on storage nodes and returns 200 if the bucket exists, 404 if not.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)

    C->>QP: HEAD /{bucket}
    QP->>SN: Check bucket in bucket table
    SN->>RDB: Lookup bucket key
    RDB-->>SN: Found / Not Found
    SN-->>QP: Result
    alt Bucket exists
        QP-->>C: 200 OK
    else Bucket not found
        QP-->>C: 404 Not Found
    end
```

---

## 7. List Objects in Bucket

**Route:** `GET /{bucket}?prefix=...&max-keys=...`

The QP streams metadata from all storage nodes using a prefix range scan on the RocksDB metadata table. Because objects in the same bucket may be spread across different erasure sets (based on key hashing), all nodes must be queried. Conflicting versions are resolved using the same read-quorum semantics as GET Object.

See [`Database.streamMetadataWithPrefix()`](../storage-node/src/main/java/com/github/koop/storagenode/db/Database.java:41).

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant SN as Storage Nodes (×9)
    participant RDB as RocksDB (each SN)

    C->>QP: GET /{bucket}?prefix=animals/
    par Stream metadata from all nodes
        QP->>SN: Stream metadata with prefix "animals/"
        SN->>RDB: Range scan metadata table (sorted by key)
        RDB-->>SN: Metadata entries
        SN-->>QP: Stream of ObjectSummary records
    end
    QP->>QP: Merge + deduplicate results
    Note over QP: Conflicting versions → apply read-quorum semantics
    QP-->>C: 200 OK [XML ListBucketResult]
```

---

## 8. Multipart Upload — Create

**Route:** `POST /{bucket}/{key}?uploads`

Initiates a multipart upload session. The QP generates a unique `uploadId` and stores the session state in the cache (Redis in production, in-memory for dev/test). No data is written to storage nodes at this stage.

See [`MultipartStorageService.initiateMultipartUpload()`](../query-processor/src/main/java/com/github/koop/queryprocessor/gateway/StorageServices/MultipartStorageService.java) and the [multipart upload plan](../plans/multipart-upload-plan.md).

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Cache as Cache (Redis / MemoryCache)

    C->>QP: POST /{bucket}/{key}?uploads
    QP->>QP: Generate uploadId = UUID
    QP->>Cache: Store session → mpu:session:{uploadId} (status=ACTIVE)
    QP->>Cache: Init empty parts set → mpu:parts:{uploadId}
    QP-->>C: 200 OK [XML InitiateMultipartUploadResult with uploadId]
```

---

## 9. Multipart Upload — Upload Part

**Route:** `PUT /{bucket}/{key}?partNumber=N&uploadId=X`

Each part is stored as an independent erasure-coded object on the storage nodes using a derived key. The cache is updated only **after** the storage nodes confirm the shard write, ensuring the client is not ACKed until the part is durably stored.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Cache as Cache (Redis / MemoryCache)
    participant SW as StorageWorker
    participant SN as Storage Nodes (×9)

    C->>QP: PUT /{bucket}/{key}?partNumber=N&uploadId=X [part data]
    QP->>Cache: Check mpu:session:{uploadId} exists
    alt Session not found
        QP-->>C: 400 Bad Request (No such upload)
    end
    QP->>Cache: Check mpu:parts:{uploadId} for partNumber N
    alt Part already uploaded
        QP-->>C: 409 Conflict (Part already uploaded)
    end
    QP->>SW: put(requestId, bucket, "{bucket}-{key}-mpu-{uploadId}-part-{N}", data)
    SW->>SN: Stream erasure-coded shards concurrently
    SN-->>SW: ACKs (≥6 required)
    SW-->>QP: Success
    QP->>Cache: Add partNumber N → mpu:parts:{uploadId}
    QP-->>C: 200 OK
```

---

## 10. Multipart Upload — Complete

**Route:** `POST /{bucket}/{key}?uploadId=X`

The QP verifies all declared parts are present in the cache, then assembles the final object by concatenating part streams and issuing a single `put` to the storage nodes under the original key. Part shards are cleaned up asynchronously.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Cache as Cache (Redis / MemoryCache)
    participant SW as StorageWorker
    participant SN as Storage Nodes (×9)

    C->>QP: POST /{bucket}/{key}?uploadId=X [XML part list]
    QP->>Cache: Verify session exists + status=ACTIVE
    QP->>Cache: Verify all declared part numbers in mpu:parts:{uploadId}
    alt Missing parts
        QP-->>C: 500 Internal Error (missing parts)
    end
    QP->>Cache: Mark session status=COMPLETING
    loop For each part in order
        QP->>SW: get(requestId, bucket, "{bucket}-{key}-mpu-{uploadId}-part-{N}")
        SW->>SN: Fetch shards + erasure-decode
        SN-->>SW: Part data stream
        SW-->>QP: Part InputStream
    end
    QP->>QP: Concatenate all part streams
    QP->>SW: put(requestId, bucket, key, totalLength, concatenatedStream)
    SW->>SN: Stream final erasure-coded shards
    SN-->>SW: ACKs (≥6 required)
    SW-->>QP: Success
    QP->>SW: delete each part shard (async cleanup)
    QP->>Cache: Remove mpu:session:{uploadId} + mpu:parts:{uploadId}
    QP-->>C: 200 OK [XML CompleteMultipartUploadResult with ETag]
```

---

## 11. Multipart Upload — Abort

**Route:** `DELETE /{bucket}/{key}?uploadId=X`

The session is immediately marked as `ABORTING` and the client is ACKed. Part shard deletion from storage nodes is performed asynchronously to avoid blocking the client.

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant QP as Query Processor
    participant Cache as Cache (Redis / MemoryCache)
    participant SW as StorageWorker
    participant SN as Storage Nodes (×9)

    C->>QP: DELETE /{bucket}/{key}?uploadId=X
    QP->>Cache: Mark mpu:session:{uploadId} status=ABORTING
    QP-->>C: 204 No Content (ACK immediately)

    Note over QP,SN: Async cleanup (does not block client)
    QP->>Cache: Read mpu:parts:{uploadId}
    loop For each uploaded part
        QP->>SW: delete(requestId, bucket, "{bucket}-{key}-mpu-{uploadId}-part-{N}")
        SW->>SN: DELETE /store/{partition}/{key}
        SN-->>SW: ACK
    end
    QP->>Cache: Remove mpu:session:{uploadId} + mpu:parts:{uploadId}
```

---

## 12. Storage Node Repair Flow

When a storage node comes back online after missing operations, it detects the gap by comparing the sequence number it last processed against the sequence number of the next incoming operation. It enters repair mode, broadcasts to peer nodes for the missed operations, and replays them — skipping any keys that have already been updated since the node came back online.

```mermaid
flowchart TD
    A[Storage Node receives operation with seq N] --> B{N == last_seq + 1?}
    B -- Yes --> C[Process operation normally]
    B -- No --> D[Enter Repair Mode]
    D --> E[Continue processing new operations\nTrack keys modified since coming back online]
    D --> F[Broadcast to peer nodes:\nRequest ops from last_seq+1 to N-1]
    F --> G[Peer nodes respond with missed operations]
    G --> H{For each missed operation:\nDoes key conflict with post-recovery ops?}
    H -- Yes --> I[Skip repair op for this key\nPost-recovery version takes precedence]
    H -- No --> J[Apply missed operation to RocksDB\nOpLog + Metadata]
    J --> K[Increment last known sequence number]
    I --> K
    K --> L{All missed ops replayed?}
    L -- No --> H
    L -- Yes --> M[Exit Repair Mode]
```

---

## 13. Gossip-Based Garbage Collection

Storage nodes periodically gossip their current sequence number (and the lowest sequence number of any active GET in flight). Any shard data associated with a sequence number below the global minimum is safe to physically delete from disk.

```mermaid
sequenceDiagram
    participant SN1 as Storage Node 1
    participant SN2 as Storage Node 2
    participant SN3 as Storage Node 3…9
    participant Disk as Disk (each SN)

    loop Periodic gossip interval
        SN1->>SN2: Gossip min(my_seq, lowest_active_GET_seq)
        SN1->>SN3: Gossip min(my_seq, lowest_active_GET_seq)
        SN2->>SN1: Gossip min(my_seq, lowest_active_GET_seq)
        SN2->>SN3: Gossip min(my_seq, lowest_active_GET_seq)
    end

    Note over SN1,SN3: Each node computes global_min = min of all received values

    SN1->>Disk: Physically delete shards with seq < global_min
    SN2->>Disk: Physically delete shards with seq < global_min
    SN3->>Disk: Physically delete shards with seq < global_min
    Note over SN1,Disk: Shards at seq ≥ global_min are retained\n(may be needed by in-flight GETs)
```

---

## RocksDB Table Reference

Each storage node maintains three tables in RocksDB, written atomically on every PUT/DELETE/bucket operation:

| Table | Key | Value | Purpose |
|---|---|---|---|
| **OpLog** | Sequence Number | `(key, operation)` | Ordered log of all mutations; enables repair |
| **Metadata** | Object Key | `(partition, seq, location)` | Latest shard location per object key |
| **Buckets** | Bucket Name | `(partition, seq)` | Bucket existence and tombstone tracking |

See [`Database.atomicallyUpdate()`](../storage-node/src/main/java/com/github/koop/storagenode/db/Database.java:23) for the atomic write implementation.
