# Distributed System Rationale

## Scalability

Storing all data on a single node is problematic; utilizing additional machines provides more storage capacity.

Fixed partitions, or virtual nodes, allow data to be split across nodes when adding capacity, avoiding the need to repartition everything.

## Durability

Relying on a single node risks complete data loss if that node experiences a failure.

The system addresses this durability concern through erasure coding.

## Throughput

Separating writers and readers from the storage nodes allows the system to scale its IO capabilities.

Utilizing multiple nodes significantly increases overall throughput.

This throughput increase aligns with the benefits of system scalability.

# Distributed System Challenges & Solutions

## Fault Tolerance

- **Challenge:** Distributing data across nodes means the system must handle individual node failures.
- **Solution:** Erasure coding allows the system to tolerate a specified number (`k`) of node failures.
- **Benefit:** Erasure coding saves space compared to standard redundant data replication.

## Recovery & Retries

- When a node returns online, it initiates a repair procedure utilizing durable message delivery via Kafka, or by reading from other nodes as a backup.
- If an internal write or delete operation fails, the system retries the operation internally.
- If the gateway goes down, the client handles the retry from the S3 side.

### Offline Nodes

- The system is fault-tolerant and continues to operate even if nodes are missing.
- Returning nodes can retrieve missed operations from Kafka or other nodes.

## Write Consistency

- **Challenge:** Ensuring shards are successfully written to a write quorum of nodes.
- **Solution:** The system employs a two-phase commit strategy.

The shards are first uploaded to the nodes; once all are uploaded, a commit message is dispatched via Kafka.

## PUT/DELETE Ordering

- **Challenge:** Concurrent writes, such as two PUTs to the same key, can lead to inconsistent states if storage nodes apply them in different orders.
- **Solution:** Kafka sequencing assigns a sequence number for each operation within a data partition.
- **Benefit:** Regardless of arrival time, nodes know how requests compare to one another, allowing the system to logically resolve the latest commit.

## Read Consistency & Version Conflicts

- **Challenge:** Different nodes may return conflicting versions of data during a read.
- **Solution:** The system returns the highest version agreed upon by a quorum.

### Handling Stale Data

- If a version lacks a quorum, it is not considered fully acknowledged or logically valid.
- In cases of missing quorum, the highest quorum version is returned instead.

### Garbage Collection

- Consensus among storage nodes determines the latest version that is safe to delete, utilizing a gossip-based watermark.

## Data Version Overload

- **Challenge:** Versioning can result in stale object versions consuming too much space over time.
- **Solution:** Nodes gossip to determine the lowest version level still actively in use.
- **Cleanup:** Any node possessing a version lower than the active baseline deletes that data.

## Multi-Stage Operation Consistency

- **Challenge:** Maintaining consensus for multi-stage operations, such as multipart uploads.
- **Solution:** A Redis cache is utilized to store the ongoing status of multipart uploads.

## Bucket Item Consistency

- **Challenge:** Storage nodes commit in order, but not always simultaneously, potentially affecting bucket item consistency across nodes.
- **Solution:** When streaming items in a bucket, an item is considered to be in the system if it is present on at least a write quorum of nodes.