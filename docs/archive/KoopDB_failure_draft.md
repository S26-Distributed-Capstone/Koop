**Distributed System Failure Analysis**

KoopDB: An S3-Compatible Erasure-Coded Object Store

Michael Kupferstein, Yonatan Ginsburg, Eitan Leitner

S26 Distributed Systems Capstone

May 2026

*This document catalogs the failure modes encountered in a distributed, erasure-coded object storage system and details the mechanisms KoopDB employs to detect, tolerate, and recover from each class of failure. All analysis references the implemented codebase.*

# **1\. Introduction**

KoopDB is a distributed, S3-compatible object storage system that shards data across a cluster of storage nodes using Reed-Solomon erasure coding. By design, the system must tolerate partial failures—network partitions, node crashes, and concurrent mutations—while preserving data integrity and maintaining linearizable ordering for mutating operations.

This document systematically examines each class of failure that can occur during the lifecycle of objects and buckets within KoopDB. For each scenario, we identify the general distributed systems concern, detail the implementation-level resolution with precise code references, and enumerate the test coverage that validates correctness under failure.

# **2\. System Architecture Baseline**

KoopDB employs an **n \= k \+ m** erasure coding scheme where **k** denotes the number of data shards and **m \= n − k** denotes the number of parity shards. The system tolerates up to **m** simultaneous node failures while still being able to reconstruct the original object from any **k** available shards. The ErasureCoder class (common-lib) implements this via the Backblaze Reed-Solomon library.

All mutating operations (PUT, DELETE, CreateBucket, DeleteBucket, MultipartCommit) are sequenced through **Kafka** on a per-partition topic (partition-{N}), providing total ordering within each data partition. The CommitCoordinator on the Query Processor publishes commit messages and blocks until a configurable **write quorum** of Storage Nodes acknowledge the commit (default timeout: 60 seconds). Storage Nodes apply these commits atomically to RocksDB.

### **Deployment Configurations**

| Parameter | Docker Compose Default | Integration Test Default |
| :---- | :---- | :---- |
| n (total shards) | 6 | 9 |
| k (data shards) | 4 | 6 |
| m (parity shards) | 2 | 3 |
| Write Quorum | 5 | 7 |
| Failure Tolerance | 2 nodes | 3 nodes |
| ACK Timeout | 60s | 10s (tests) |

### **Two-Phase Write Protocol**

Every PUT operation follows a strict two-phase protocol implemented in StorageWorker.put():

1. **Phase 1 — Shard Upload:** The Query Processor erasure-encodes the object into *n* shards and streams them concurrently to all Storage Nodes via HTTP PUT. The QP waits for at least *writeQuorum* shard upload acknowledgments before proceeding.

2. **Phase 2 — Commit:** The CommitCoordinator publishes a FileCommitMessage to the partition’s Kafka topic. Each Storage Node that receives the message atomically writes an OpLog entry and a Metadata entry to RocksDB, then POSTs an ACK to the coordinator’s HTTP endpoint. The QP blocks until *writeQuorum* commit ACKs arrive or the timeout elapses.

This separation ensures that shard data is durably placed before the ordered metadata commit is broadcast, preventing the system from committing a reference to data that does not yet exist on a quorum of nodes.

# **3\. Bucket Creation Failures**

## **3.1 General Distributed Concern**

Reaching consensus on bucket creation across a distributed cluster can fail due to quorum timeouts when an insufficient number of nodes acknowledge the creation. Additionally, distributed systems must handle idempotent retries: a client or load balancer may reissue a CreateBucket request after a transient timeout, even though the first request succeeded on some nodes.

## **3.2 Implementation & Resolution**

Bucket creation is sequenced through Kafka to guarantee total ordering against concurrent operations. The StorageWorker.createBucket() method hashes the bucket name to a partition via ErasureRouting.getPartition() (CRC32 hash), then delegates to CommitCoordinator.beginCreateBucket(). A CreateBucketMessage is published to the partition’s Kafka topic, and the coordinator blocks until **k \+ 1** Storage Nodes ACK the commit.

On the Storage Node side, StorageNodeServerV2.processSequencerMessage() invokes StorageNodeV2.createBucket(), which calls Database.createBucket(). This method atomically writes both an OpLog entry (Operation.CREATE\_BUCKET) and a Bucket table entry within a single RocksDB transaction. The Bucket record stores the bucket name, partition, sequence number, and a deleted flag (initially false).

**Idempotency:** If a CreateBucket message arrives at a node that already has a non-deleted Bucket record for the same name, the node simply overwrites the record with the new sequence number and re-ACKs. Since RocksDB’s put is an upsert, duplicate creates are inherently idempotent at the storage layer.

## **3.3 Failure Modes**

| Failure Mode | Behavior | Resolution |
| :---- | :---- | :---- |
| Quorum timeout | Fewer than k+1 nodes ACK within 60s | CommitCoordinator returns false; QP returns 500 to client. Client retries. |
| Partial commit | Some nodes committed, others did not receive the Kafka message | Committed nodes have the bucket; uncommitted nodes will receive the message when they catch up via Kafka replay. |
| Duplicate request | Same bucket name resubmitted | Idempotent upsert in RocksDB; no conflict. |

## **3.4 Test Coverage**

StorageWorkerApiTest.createBucket\_succeedsAndPublishesToPartitionTopic() — Verifies the full pub/sub path: message publication, topic selection, and quorum ACK.

StorageWorkerApiTest.createBucket\_failsWhenBelowQuorum() — Disables 3 of 9 nodes and verifies that createBucket() returns false when only 6/9 ACKs arrive (below k+1=7 threshold), using a 3-second timeout.

RealStorageNodesIT.createBucket\_realServers() — End-to-end test with real RocksDB-backed storage nodes confirming the full CreateBucketMessage → SN commit → ACK path.

# **4\. Bucket Deletion Failures**

## **4.1 General Distributed Concern**

Deleting a container that still holds objects creates race conditions: a concurrent PUT may land in a bucket that is being deleted. A partial quorum might result in some nodes deleting the bucket while others retain it, leading to cluster inconsistency on subsequent HeadBucket or ListObjects calls.

## **4.2 Implementation & Resolution**

Bucket deletion follows the same Kafka-sequenced pattern as creation. StorageWorker.deleteBucket() publishes a DeleteBucketMessage and requires **k \+ 1** ACKs. On each Storage Node, Database.deleteBucket() atomically writes a **tombstone** Bucket record (deleted \= true) and an OpLog entry within a single RocksDB transaction.

Concurrent writes versus deletes are strictly ordered by the Kafka sequencer: since both CreateBucket, DeleteBucket, and object PUT/DELETE all flow through per-partition Kafka topics, the sequence numbers establish a total order. If a PUT arrives after a DeleteBucket, the PUT’s sequence number is higher, and the object is correctly stored. If a PUT arrives before a DeleteBucket, the object’s metadata exists at a lower sequence number and is unaffected by the bucket tombstone.

**Tombstone reconciliation:** The StorageNodeV2.bucketExists() method calls Database.bucketExists(), which returns false for any Bucket record where deleted \== true. On the QP side, StorageWorker.bucketExists() queries all nodes in the partition’s erasure set concurrently and returns true if *any* node reports the bucket as existing. This prevents false negatives when a stale node has not yet processed the tombstone.

## **4.3 Test Coverage**

StorageWorkerApiTest.deleteBucket\_succeedsAndPublishesToPartitionTopic() — Verifies message publication and quorum ACK for the delete path.

StorageWorkerApiTest.bucketLifecycle\_createPutDeleteObjectDeleteBucket() — Full lifecycle: create → PUT object → GET → DELETE object → DELETE bucket, all against real fake-SN servers with commit protocol.

StorageNodeServerV2Test.testHeadBucket\_returnsNotFoundAfterBucketDeleted() — Confirms the tombstone is respected by the HEAD endpoint after deletion.

# **5\. Listing Objects Under Failure**

## **5.1 General Distributed Concern**

Listing objects in a highly concurrent environment can expose read-your-writes consistency gaps. Mid-upload objects may appear partially, or deleted objects (stale tombstones) might linger on isolated nodes. Additionally, because objects in the same bucket may be spread across different erasure sets based on key hashing, a listing operation must fan out to all partitions.

## **5.2 Implementation & Resolution**

The StorageWorker.listObjects() method fans out a GET request to one node per partition across all partition spreads, merges and deduplicates results by key, sorts alphabetically, and applies the maxKeys limit. On each Storage Node, StorageNodeServerV2.handleListObjects() performs a RocksDB prefix scan via Database.listItemsInBucket() and dynamically filters out entries whose latest version is a TombstoneFileVersion. This filtering happens at the storage layer, ensuring deleted objects never appear in listings regardless of the node’s compaction state.

For unmaterialized versions (commit received but blob not yet on disk), the listing still includes the key because the metadata record exists. This is consistent with the S3 contract: once a PUT is acknowledged, the object is listable. The physical file will either arrive via the delayed shard upload or be reconstructed by the RepairWorkerPool.

## **5.3 Test Coverage**

StorageNodeServerV2Test.testListObjects\_excludesDeletedObjects() — Commits two objects, deletes one via tombstone, and verifies the listing excludes the deleted key.

StorageWorkerApiTest.listObjects\_returnsStoredObjects() — Verifies end-to-end listing across the fake 9-node cluster after creating a bucket and storing multiple objects.

S3GatewayE2EIT.e2e\_listObjects\_withPrefix\_filtersResults() — Full-stack S3 SDK test confirming prefix filtering returns only matching keys.

# **6\. Object Upload (PUT) Failures**

## **6.1 General Distributed Concern**

Data ingest is a multi-phased process prone to partial failures: a node may fail mid-upload causing incomplete shard placement, network drops may prevent the commit from reaching quorum, or clients may retry and create overwrite races with interleaved chunks from different versions.

## **6.2 Implementation & Resolution**

The PUT path in StorageWorker.put() implements the two-phase protocol described in Section 2\. Phase 1 streams erasure-coded shards concurrently to all *n* nodes via java.net.http.HttpClient with a virtual-thread executor. If any shard upload fails (node down, network timeout), the encoder’s ThreadSafePipe marks the stream as dead and continues writing to surviving streams. The QP requires at least **writeQuorum** successful shard uploads before proceeding to Phase 2\.

**Shard upload failure:** If fewer than *writeQuorum* shards are uploaded, Phase 1 fails immediately and the QP returns an error to the client. The partially uploaded shards are orphaned on disk but are not referenceable because no commit message was ever published. These orphans are cleaned up by background garbage collection.

**Commit miss (blob arrives after commit):** When a Storage Node receives a FileCommitMessage via Kafka but does not yet have the corresponding shard on disk, StorageNodeV2.commit() returns false (the materialized flag). The StorageNodeServerV2.processSequencerMessage() handler detects this and enqueues a RepairOperation into the RocksDbRepairQueue. The RepairWorkerPool asynchronously fetches shards from peer nodes, reconstructs the original data via Reed-Solomon, re-shards to extract the local shard, and writes it to disk.

**Overwrite races:** Concurrent PUTs to the same key are deterministically resolved by Kafka sequence numbers. Each PUT publishes a FileCommitMessage with a unique requestId. Storage Nodes append each version to the key’s Metadata.versions list. On GET, the QP selects the highest sequence number that a read quorum agrees on.

## **6.3 Repair Queue Architecture**

The repair subsystem uses a **persistent RocksDB-backed queue** (RocksDbRepairQueue) to survive node restarts. The queue implements last-writer-wins compaction: if the same key is enqueued multiple times, only the operation with the highest seqOffset is retained. The RepairWorkerPool polls the queue periodically, checks the WriteTracker to avoid conflicting with in-progress uploads, and dispatches repair tasks to virtual-thread workers.

On node restart, the RocksDbRepairQueue constructor calls rebuildIndex(), which scans the persisted repair entries, deduplicates by blob key, and resumes processing. This ensures that repairs initiated before a crash are not lost.

## 

## 

## **6.4 Test Coverage**

| Test | Validates |
| :---- | :---- |
| StorageWorkerApiTest.putThenGet\_roundTrip() | Basic 15 MB round-trip through 9-node cluster with commit protocol |
| StorageWorkerApiTest.get\_withThreeNodeFailures\_stillWorks() | Reconstructs data with 3/9 nodes disabled after PUT |
| StorageWorkerApiTest.commitPhase\_failsWhenBelowQuorum() | Verifies PUT fails when only 6/9 nodes ACK (below writeQuorum=7) |
| StorageWorkerApiTest.commitPhase\_succeedsWithUploadFailureThatLaterAcks() | 2 nodes fail shard upload but ACK commit; PUT succeeds |
| StorageNodeRepairTest.testCommitMissEnqueuesRepairWhenBlobAbsent() | Commit without prior PUT enqueues repair operation |
| StorageNodeRepairTest.testActualBlobRepair() | Full E2E: commit miss → peer fetch → Reed-Solomon reconstruct → local shard write |
| RepairWorkerPoolTest.testPersistentQueueSurvivesMultipleRestartCycles() | Enqueued repairs survive 3 restart cycles of RocksDB \+ pool |

# **7\. Object Download (GET) Failures**

## **7.1 General Distributed Concern**

Reads must retrieve intact, uncorrupted data even if some nodes are completely dead, severely lagging, or currently processing concurrent overwrites or deletions. The system must also handle version conflicts: different nodes may report different versions of the same key if a write is mid-commit.

## **7.2 Implementation & Resolution**

The GET path in StorageWorker.get() queries all *n* Storage Nodes concurrently for the shard associated with the requested key. Each node returns its shard data along with two custom headers: X-Koop-Version (the Kafka sequence number) and X-Koop-Type (BLOB, MULTIPART, or TOMBSTONE).

### **Version Reconciliation Algorithm**

The QP collects all shard responses and groups them by version (sequence number). It then selects the **highest version** reported by any node. If the highest version has at least *k* shards, the QP reconstructs from those shards via ErasureCoder.reconstruct(). If the highest version has fewer than *k* shards (indicating a mid-commit write), the QP falls back to the **second-highest version** and attempts reconstruction from that version’s shards.

### **Failure Resolution by Type**

1. **Node unavailable:** Up to *m* nodes can be completely offline. As long as *k* shards are available, reconstruction succeeds. This is validated by get\_withThreeNodeFailures\_stillWorks().

2. **Tombstone detection:** If the latest version’s X-Koop-Type header is TOMBSTONE, the QP returns null (mapped to HTTP 404 by the gateway). No reconstruction is attempted.

3. **Multipart object:** If the type is MULTIPART, the QP reads the chunk list from the response body and sequentially fetches each chunk via handleMultipartGet(), reconstructing each chunk independently.

4. **Insufficient shards:** If fewer than *k* shards are available for any version, the GET fails with HTTP 500\. This is validated by get\_withFourNodeFailures\_fails().

## **7.3 Test Coverage**

RealStorageNodesIT.get\_tolerates\_three\_node\_failures\_realServers() — Confirms reconstruction with 3/9 nodes stopped.

RealStorageNodesIT.get\_fails\_with\_four\_node\_failures\_realServers() — Confirms reconstruction fails with 4/9 nodes stopped.

RealStorageNodesIT.get\_blob\_version\_fallback\_realServers() — Simulates a partial v2 on only 3 nodes; verifies the QP falls back to v1 with 9 shards.

RealStorageNodesIT.get\_after\_delete\_returnsNull\_realServers() — Writes a tombstone to all 9 DBs and confirms GET returns null.

# **8\. Object Deletion Failures**

## **8.1 General Distributed Concern**

Deletions racing against reads or new uploads can cause “ghost” files to appear (a delete is processed before a read completes, corrupting the reconstruction) or legitimate new files to be accidentally masked by delayed, obsolete delete commands.

## **8.2 Implementation & Resolution**

Object deletion in StorageWorker.delete() publishes a DeleteMessage to the partition’s Kafka topic and waits for **k \+ 1** ACKs. On each Storage Node, Database.deleteItem() atomically appends a TombstoneFileVersion to the key’s version list and writes an OpLog entry (Operation.DELETE), all within a single RocksDB transaction.

The tombstone’s Kafka sequence number acts as an **immutable logical clock**. Because all mutations for a given partition are totally ordered by Kafka, a tombstone with sequence number *S* can never mask a PUT with sequence number *S’ \> S*. The QP’s version reconciliation (Section 7.2) always selects the highest sequence number, so a new PUT after a delete correctly supersedes the tombstone.

**Physical shard cleanup:** The actual shard files remain on disk after a tombstone is written. Physical deletion is deferred to background garbage collection, which is designed (but not yet fully implemented) to use gossip-based watermarking to determine the safe deletion threshold.

## **8.3 Test Coverage**

StorageWorkerApiTest.delete\_publishesToPartitionTopicAndAwaitsQuorum() — Verifies the DeleteMessage is published and ACKed.

StorageWorkerApiTest.productionConstructor\_putGetDelete\_roundTrip() — Full lifecycle: PUT → GET → DELETE → GET (returns null).

S3GatewayE2EIT.e2e\_putObject\_thenDeleteObject\_thenGet\_returns404() — S3 SDK test confirming GET returns NoSuchKeyException after DELETE.

# **9\. Multipart Upload Failures**

## **9.1 General Distributed Concern**

Handling massive files requires chunking. Systems must manage the state of these chunks across potentially multiple stateless gateway instances, precisely coordinate their finalization into a single logical object, and clean up orphaned chunks on upload failure. Race conditions between concurrent complete/abort operations and between part uploads and session lifecycle transitions are particularly challenging.

## **9.2 Session State Management**

The MultipartUploadManager tracks session state via a CacheClient abstraction (MemoryCacheClient for dev/test, RedisCacheClient for production multi-QP deployments). Each session transitions through a strict lifecycle:

ACTIVE → COMPLETING → (session deleted on success) or ACTIVE → ABORTING → (session deleted after cleanup).

Part numbers are tracked in a Redis/memory **set** (mpu:parts:{uploadId}). The setAddIfAbsent() operation atomically checks both that the set exists (session not aborted) and that the part number is not already present (no duplicate uploads). In the RedisCacheClient, this is implemented as a Lua script for atomicity.

## **9.3 Failure Modes & Resolution**

### **Part Upload Failure**

If StorageWorker.put() fails during a part upload, the MultipartUploadManager.uploadPart() method removes the part number from the cache set (cache.setRemove()) and returns a STORAGE\_FAILURE result. This ensures the part number is not marked as uploaded when the shard data is not durably stored.

### 

### 

### **Session Aborted Mid-Upload**

After a successful StorageWorker.put() for a part, the manager re-checks cache.exists(sessionKey) to detect if the session was concurrently aborted. If the session key is gone, the manager removes the part from the set and issues a best-effort storageWorker.delete() to clean up the orphaned shard.

### **Concurrent Complete Race**

Two threads racing to complete the same upload are serialized by the ACTIVE → COMPLETING transition. The transitionToCompleting() method uses cache.putIfPresent() (Redis SET XX) to atomically check-and-update the session status. Only the first thread to transition the session to COMPLETING proceeds; the second sees a non-ACTIVE status and returns CONFLICT. This is validated by MultipartUploadIT.multipartUpload\_concurrentComplete\_onlyOneSucceeds().

### **Abort Racing Against Active Uploads**

The abort handler immediately marks the session as ABORTING and then deletes the parts set (cache.setDelete()). Any concurrent uploadPart() call will either fail the setAddIfAbsent() check (set no longer exists) or fail the post-upload cache.exists(sessionKey) check. This ensures no new parts are accepted after an abort, even under concurrent execution. Validated by MultipartUploadIT.multipartUpload\_abortDuringActiveUploads\_noCorruption().

### **Commit Failure and Retry**

If the storageWorker.beginMultipartCommit() call fails (Kafka unreachable, quorum timeout), the manager restores the session to ACTIVE via restoreSessionToActiveIfStillCompleting(), allowing the client to retry the complete operation. This is validated by MultipartUploadManagerTest.complete\_storeFailure\_canRetryWithoutStuckCompleting().

## **9.4 Test Coverage Summary**

| Test | Scenario |
| :---- | :---- |
| MultipartUploadManagerTest (12 tests) | Unit tests: initiate, upload, complete, abort, cache invariants, concurrency, retry |
| MultipartUploadIT (13 tests) | Integration tests with real 9-node cluster: full lifecycle, out-of-order parts, abort cleanup, duplicate rejection, concurrent uploads, node failure mid-upload, concurrent complete race |
| S3GatewayE2EIT (3 multipart tests) | Full S3 SDK → Javalin gateway → StorageWorker → real SNs: lifecycle, abort → 404, large file round-trip |

# **10\. Node Failure & Repair**

## **10.1 General Distributed Concern**

In a distributed storage system, individual nodes will inevitably crash, lose network connectivity, or lag behind. The system must continue serving reads and writes during the outage, and when the node returns, it must catch up on missed operations without data loss or duplication.

## 

## **10.2 Erasure Coding Tolerance**

KoopDB’s erasure coding scheme provides **transparent read tolerance**: any *k* of *n* shards suffice for reconstruction. This means up to *m \= n − k* nodes can be completely offline during a GET without any degradation in data integrity. The ErasureCoder.reconstruct() method accepts a boolean\[\] present array indicating which shards are available and performs Reed-Solomon decoding on the available subset.

## **10.3 Write Tolerance**

During writes, the system requires *writeQuorum* shard uploads in Phase 1\. Nodes that miss the shard upload can still ACK the Phase 2 commit (they receive the Kafka message and record the metadata, but the blob is not materialized). The StorageNodeV2.commit() method returns false in this case, triggering the repair path.

## **10.4 Repair Architecture**

The repair subsystem consists of three components:

1. **RocksDbRepairQueue** — A persistent, crash-safe queue backed by a dedicated RocksDB column family. Enqueued operations survive node restarts. Implements last-writer-wins compaction: if the same blob key is enqueued with a higher seqOffset, the older entry is deleted.

2. **RepairWorkerPool** — A virtual-thread pool that periodically polls the queue, checks WriteTracker for in-progress uploads (to avoid conflicts), and dispatches repair tasks.

3. **BlobRepairStrategy** (StorageNodeServerV2::repairBlob) — The actual repair logic: determines the partition via ErasureRouting, fetches shards from peer nodes, reconstructs the full object via ErasureCoder.reconstruct(), re-shards via ErasureCoder.shard() to extract the local shard, and writes it to disk via StorageNodeV2.store().

## **10.5 Repair Flow**

1. Storage Node receives a FileCommitMessage via Kafka for key K.

2. StorageNodeV2.commit() checks the uncommitted-writes table for the requestId. If absent, returns false (not materialized).

3. StorageNodeServerV2 enqueues a RepairOperation(K, seqNumber, requestId) into RocksDbRepairQueue.

4. RepairWorkerPool polls the queue, finds the operation, confirms no active write for K (via WriteTracker), and dispatches to a virtual-thread worker.

5. The worker fetches shards from *n − 1* peer nodes via HTTP GET, reconstructs the full object, re-shards, and writes the local shard to disk.

6. On success, the operation is removed from the queue. On failure, it remains for automatic retry on the next poll cycle.

## **10.6 Test Coverage**

StorageNodeRepairTest.testActualBlobRepair() — Full E2E repair with mocked peer nodes serving real erasure-coded shards.

RepairWorkerPoolTest.testPersistentQueueSurvivesMultipleRestartCycles() — Enqueues operations, shuts down, reopens RocksDB, confirms operations survive and execute.

RepairWorkerPoolTest.testActiveWriteCheckDefersRepair() — Marks a key as actively writing; confirms repair is deferred until the write completes.

RepairWorkerPoolTest.testLastWriterWinsExecutesLatestOperation() — Enqueues the same key twice; confirms only the higher seqOffset executes.

# **11\. Summary of Failure Tolerance Mechanisms**

| Operation | Ordering Mechanism | Quorum Requirement | Failure Recovery |
| :---- | :---- | :---- | :---- |
| CreateBucket | Kafka per-partition sequencing | k \+ 1 ACKs | Idempotent upsert; Kafka replay |
| DeleteBucket | Kafka per-partition sequencing | k \+ 1 ACKs | Tombstone marker; Kafka replay |
| PUT Object | Two-phase: shard upload \+ Kafka commit | writeQuorum (Phase 1 \+ Phase 2\) | RepairWorkerPool reconstructs missing shards |
| GET Object | Version reconciliation (highest seq\#) | k shards (read quorum) | Fallback to earlier version if latest insufficient |
| DELETE Object | Kafka per-partition sequencing | k \+ 1 ACKs | Tombstone with seq\# prevents stale masking |
|  Multipart Upload |  Cache state \+ Kafka commit |  writeQuorum per part \+ commit |  CAS transitions; retry on  commit failure |
| Node Crash | N/A | N/A | Persistent repair queue; erasure reconstruction |

# **12\. Code References**

All class and method references in this document correspond to the KoopDB codebase at the following module paths:

* **Common Library:** common-lib/src/main/java/com/github/koop/common/

* **Query Processor:** query-processor/src/main/java/com/github/koop/queryprocessor/

* **Storage Node:** storage-node/src/main/java/com/github/koop/storagenode/

* **System Tests:** system-tests/src/test/java/koop/

For the complete API surface and deployment guide, see docs/api.md and docs/installation.md in the project repository.

