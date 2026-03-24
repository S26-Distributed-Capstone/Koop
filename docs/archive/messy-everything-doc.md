

# The Full Flushed Out Idea

The idea flushed out:  
**Problem \#1:** Basically, we need to sequence put/delete operations. Consider the following case:

1) QP \#1: PUT animals/dog.jpg \[data 1\]  
2) QP \#2: PUT animals/dog.jpg \[data 2\]

	Each QP sends the requests to the erasure set at the same time \- how do we ensure that the 9 or so nodes in the erasure set all end up with EITHER the shard for \[data 1\] OR \[data 2\] but not both? This is a consistency problem  
	The solution is to maintain some sequence for PUT/DELETE operations. This is where kafka comes in. One kafka partition per data partition for per-partition sequencing  
	**Problem \#2:** Repair. Forget a node being off for a day \- lets say an erasure set node experiences a network issue and misses 20 or so PUTs. When it comes back online, how does it get those puts? Kafka read  
	Therefore, I propose we use RocksDB and track metadata for each shard as follows in three tables

Table \#1 \- operation log  
Operations \= (PUT,DELETE,CREATE\_BUCKET,DELETE\_BUCKET)  
	

| Sequence Number | Key | Operation |
| :---- | :---- | :---- |
| 98 | animals/dog.jpg | PUT |
| 99 | animals/cat.jpg | PUT |
| 100 | animals/cat.jpg | PUT |
| 5 | animals | CREATE\_BUCKET |

Table \#2 \- metadata

| Key | Partition | sequence number & locations |
| :---- | :---- | :---- |
| animals/cat.jpg | A | (100, /uuid.blob) (101,\[multipart keys\]) |
| animals/dog.jpg | A | 98 |

Table \#3 \- Buckets

| Key | Partition | Sequence Number |
| :---- | :---- | :---- |
| animals | **A** | 5 |

# Storage Node Operations/Tables above

1. PUT item or multipart item  
   1. atomic write into metadata and oplog  
2. DELETE item or multipart item  
   1. atomic write into metadata (tombstone) and oplog  
3. GET ITEM  
   1. return metadata from metadata table (whether multipart or regular)  
4. CREATE BUCKET  
   1. atomic write into bucket table and oplog  
5. DELETE BUCKET  
   1. atomic write into bucket table (tombstone) and oplog  
6. BUCKET EXISTS  
   1. check bucket in bucket table  
7. LIST ITEMS IN BUCKETS  
   1. stream through metadata in metadata table starting with the prefix of the bucket

# Repair

## Simple Repair

Storage nodes can read from kafka \- if that fails (offline or kafka lost those messages) then it can ask for sequence numbers it is missing from other storage nodes

## Log Compaction

When streaming seq nums from other nodes, if we have two PUTs on the same key can just use the latest one. Can compact partitions in parallel

## Full Catch-up

Storage node can copy the metadata table and file system of another node. Then ask other storage nodes for lowest sequence number per partition up to current.

# Gossip-Based Garbage Collection

1. Periodically gossip around min(sequence number I am up to, lowest sequence number of currently active GET)  
2. Safe to delete anything under that. Anything equal or higher might be requested

# Buckets

Buckets are just stored metadata in a rocksdb table. Existence is determined by quorum of majority of nodes

# Full PUT

1. QP streams data to all 9 storage nodes  
2. Waits for ACKS from at least 7 or 8  
   1. If ACKS are not received, retry sending to the remaining nodes (within reason)  
3. QP sends update command to kafka  
4. SN’s receive update from kafka  
   1. If it has the data  
      1. add operation to op log and metadata to metadata in rocksdb  
   2. if not  
      1. maybe it’s still being sent data from QP? If so, wait  
      2. If not (missed data stream), first **reconstruct** its shard on disk from the other nodes, and **then** update metadata in rocksdb  
5. SN’s ACK to QP  
   1. (if no acks received, redis crashed \- resend to redis)  
6. When QP receives ACKS from at least 7 or 8, return to client

# Full GET

1. QP sends get request to all 9 storage nodes  
2. IF header is multipart and a write quorum of nodes have the data…  
   1. for each part, go through and request, erasure decode, string those together yadda yadda  
3. IF not multipart  
   1. return the highest version that at least a readable amount (6/9) share  
   2. this might involve another request

# Full DELETE

1. QP publishes delete command to kafka  
2. SN receives update from kafka  
   1. tombstone in metadata and operation log in rocksdb  
3. SN’s ACK to QP  
4. When QP receives ACKS from at least 7 or 8, return to client

### GETS and conflicting versions

issue \- if on a GET there are conflicting versions between diff nodes  
**Approach 1:** **Retrieve Older Version**

* It’s appropriate to theoretically return an older version **only if** the write for the newer version has not yet ACK-ed to client  
* This is bad, because we can’t distinguish between A) an older version which was ACKed and B) an older version that was not ACKed to client. For example, for versions 1 and 2, and 3 out of 9 nodes are down and we see \[1,2,2,2,2,2\], it could be that 2 was actually ACKed to client with some of the nodes which then died. In this case, it would be inappropriate to return 1  
  * if all 2’s are down we are davka guarding against that case, so we can return older version

# Create Bucket

1. if bucket exists → exit (s3 error)  
2. send command to sequencer to create bucket  
   1. storage nodes store this in rocks\!  
3. wait for acks from a write quorum of SN’s  
4. ack to client

# List Bucket

1. Stream metadata from all the nodes  
2. if conflicting versions → mid-write. Go with our read semantics above

# Delete Bucket

1. Send update to SN’s to delete bucket in metadata (tombstone)

# Multipart Upload

## Create multipart upload

1. Put upload id → key into cache

## Upload part

1. if part exists  
   1. return error, עיין אס-טרי  
2. otherwise  
   1. upload part (ie dog.jpg-chunk1-\[multipart\_id\])  
   2. upload id in cache (part 1 uploaded for dog.jpg)  
   3. only then ack to client once ID successfully uploaded to cache

## Abort

1. Mark id in cache as DELETING  
2. ACK to CLIENT  
3. Schedule for later/now:  
   1. read parts from deleting  
   2. instruct storage nodes to delete parts

## Complete

1. Writer checks the parts against redis \- ensuring we have all uploaded  
2. Instruct the storage nodes through sequencer to store metadata for all of the parts (rocksdb)  
   1. ie dog.jpg maps to  
      1. dog.jpg-chunk 1,2,3,4 etc  
3. Wait for ACKS from SN’s  
4. ACK to client

# Overview

We are creating a distributed object store, which stores objects redundantly through **erasure encoding**: Instead of replicating objects across different nodes, objects are split up into shards \- and due to the algorithm used, we can tolerate losing some shards and still recreate the object on demand.  
This database is a pluggable layer for S3. The API for this is exactly the same as the S3 API, and is intended to be used as the backend for an app that uses S3.

# API

*Document your system from a user's perspective. Be sure to include reference documentation for any APIs you have created, as well as configuration settings that the user is expected to configure.*

Each object belongs to a **data partition**, and the data partition belongs to an erasure set. Each erasure set is made up of a number of disks which make up the erasure set. At the time of writing, each erasure set is hardcoded as 9 disks with the ability to lose 3\. We will generalize this. See configurations below on how to configure this.

We support the following operations from S3:

- Object PUT/GET  
- Bucket CREATE/DELETE  
- List objects in bucket  
- Multipart upload:  
  - create multipart upload  
  - upload part  
  - abort multipart upload  
  - complete multipart upload

The API for all of these can be found on the [S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html). The calls are completed through the HTTP/S3 client in your favorite programming language.

## Configurations

Both of the below configurations are put into etcd.  
Partition spread configuration defines which data partitions to which erasure set.

| {
"partition\_spread": \[
{
"partitions": \[0, 1, 2, 3, 4, 5, 6, 7, 8\],
"erasure\_set": 1
},
{
"partitions": \[9, 10, 11, 12, 13, 14, 15, 16, 17\],
"erasure\_set": 2
},
{
"partitions": \[18, 19, 20, 21, 22, 23, 24, 25, 26\],
"erasure\_set": 3
}
\]
} |
| :---- |

erasure set configuration defines which erasure sets map to which machines

| {
"replica\_sets": \[
{
"number": 1,
"machines": \[
{ "ip": "192.168.1.1", "port": 8080 },
{ "ip": "192.168.1.2", "port": 8080 },
{ "ip": "192.168.1.3", "port": 8080 }
\]
},
{
"number": 2,
"machines": \[
{ "ip": "192.168.1.4", "port": 8080 },
{ "ip": "192.168.1.5", "port": 8080 },
{ "ip": "192.168.1.6", "port": 8080 }
\]
},
{
"number": 3,
"machines": \[
{ "ip": "192.168.1.7", "port": 8080 },
{ "ip": "192.168.1.8", "port": 8080 },
{ "ip": "192.168.1.9", "port": 8080 }
\]
}
\]
} |
| :---- |

# Architecture

*Define your architecture using C4 or equivalent diagrams to visualize the system. Be sure to indicate where and how redundancy is used in your system. Reference workflow and the distributed challenges documents where appropriate.*

![][image1]

![][image2]  
![][image3]

# Challenges

*Identify the use cases and reasons why your system and/or components need to be distributed. Explain how and why "going distributed" solves the issues. Identify and list the resultant distributed system challenges that you need to address. For each challenge or issue, explain how you propose to address it. If you do not plan on addressing a particular challenge, explain why it is not critical and out of scope for your project. Indicate how it could be addressed in "phase two" of your project.*

* Scalability  
  * Storing everything on one node is bad, use more machines for more space  
  * Fixed partitions (virtual nodes) allow for splitting data between nodes when we add more without repartitioning everything  
* Durability  
  * Storing everything on one node is bad, if it goes down we lose everything  
  * erasure coding  
* Throughput  
  * Separating writers/readers from storage nodes allow for scaling IO  
  * Using multiple nodes provides much more throughput  
    * See scalability above

Challenges due to distributedness

* Fault tolerance  
  * Now that data is distributed across nodes, we need to be able to handle node failures \- erasure coding  
  * Erasure coding  
    * Can handle k node failures  
    * Saves space but not having redundant data  
  * When a node comes back online \- repair procedure (durable message delivery with kafka; can read from other nodes as a backup)  
  * When a write or delete fails \- retry internally  
    * S3 client side retry if gateway goes down  
* Consistency of writes (ensuring shards are written to a write quorum of nodes)  
  * **Two phase commit** \- 1\) upload the shards to the nodes, 2\) once all shards are uploaded, send the commit message via kafka  
* Consistency of Writes (PUT/DELETE Ordering)  
  * When multiple clients issue concurrent writes (e.g., two PUTs to the same key), different storage nodes may apply operations in different orders, leading to inconsistent state.  
  * Solution: kafka sequencing  
    * Gives a sequence number for each operation within a data partition so regardless of when the actual request arrives, each node knows when it falls out in comparison to other requests  
      * Latest commit

* A node may go offline and miss operations  
  * The system is fault tolerant and  can continue to operate even if its missing nodes  
  * Each node can get other operations from kafka, or as a backup other nodes  
* Read Consistency & Conflicting Versions  
  * Different nodes may return different versions  
  * Return highest version agreed upon by quorum  
    * If less then a quorum have a version it wasnt fully acked and therefore not logically a valid version of data and should return the highest quorum version (most recent acked data)  
    * Requires **consensus** among the storage nodes for the latest version safe to delete (gossip based watermark garbage collection)  
* Consensus/Consistency of multi-stage operations (multipart upload)  
  * Use redis cache to store multipart upload status  
* Too many version of data  
  * With versioning, stale versions of objects can be stored and take up space   
    * Gossip lowest version level still in use by nodes  
      * If any node has any version lower than the lowest version level still in use delete it  
* Consistency of items in bucket across nodes  
  * Issue: Storage nodes commit in order, but not necessarily at the same time  
  * Solution: When streaming items in a bucket, if \>=write quorum of nodes have it, we consider it in the system

# 

# Scope

*Define the scope of your development and what you will deliver in terms of use cases. List all the scenarios you will be supporting. Link to the workflow diagrams when appropriate. Do not forget about error cases, which will include node and service failures and the recovery that is expected.*

* At level one, *KoopDB* is a **Distributed Object Store** that provides a **S3**\-compatible API for PUT, GET, DELETE, bucket ops, and multipart support.   
  * The “*Client*” is any application that talks to the store via the standard S3 HTTP API. (i.e. the AWS sdk, boto3, minIO, etc.)  
  * Internally the store routes, sequences, and durably shards every object.  
  * KoopDB then returns S3 responses, such as data, ACKs, and error codes.  
* At level two, KoopDB is broken down into containers, with requests coming into the **Query Processor (QP),** which after processing is routed to the backend **Storage Nodes (SN)**. (FYI, the provided information on the QP is based on pending changes being made, which provides multipart support, not what's currently on main)  
  * The QP is a stateless request router, which accepts S3 API calls, streams object data to storage nodes, publishes sequences commands to Redis, and waits for quorum ACKs before replying to the client. The QP is broken into two internal layers. The **gateway** and the **processor**.   
    * Requests arrive to a *Javalin* app with all S3-compatible routes, created in gateway/main.java. It utilizes Java 21 and virtual threads to provide a gateway that is easy to scale.  
    * Internally it is created by passing in a StorageService to Main.java, this provides us with a way to a) choose which storage service we use (Local for dev/test or StoreWorker for production and e2e test) b) inject mocks for testings  
      * StorageWorkerService is an abstraction of StorageService and is responsible for communication from gateway to processor.  
        * It does this by receiving a **StorageWorker (SW)** that is passed in Main.main  
    * The SW provides a number of internal services to distribute erasure-coded shards to storage nodes via HTTP:  
      * **Etcd**: Read partition-spread and erasure-set configuration on startup and config change from etcd.  
      * **Multipart Upload Cache**: communicates to Redis to manage the state of ongoing multipart uploads across different instances.  
* At level three, the **Storage Nodes (SN)** are the final destination for object shards and the source of truth for metadata.  
  * Storage nodes use RocksDB to maintain consistent local metadata tables for object locations and operation logs.  
  * They consume from Kafka to ensure all operations are applied in the correct sequence across the erasure set.  
  * SNs are responsible for local repairs, periodic garbage collection via gossip, and participating in quorum-based reads/writes.

# 

# Technologies

*List the tools and technologies you plan on using. For each, explain why you decided to use that particular tool or technology. List other options you evaluated and explain why you chose not to use them.*

### Storage Node

1. RocksDB for internal metadata. A very fast and low-latency and simple k-v store is what we need. Also provides atomic writes between “column families” keeping our column families consistent

### Query Processor

1. Reed-Solomon java pkg  
   1. Implements the erasure encoding math

### Other

1. Kafka  
   1. Used for pub/sub from QP’s to SN’s \- sequence operations per partition for consensus and consistency across storage nodes in the erasure set  
2. Redis  
   1. Used for multipart upload cache. Could not use in memory because different QP’s might handle different parts/multipart commands  
3. Javalin for internal communication  
   1. HTTP not necessarily needed between QP and SN, and TCP technically sufficient, but needed some mechanism to multiplex stuff across the same socket. This was simplest solution.

# Workflows

*Display workflow diagrams for each of your supported use cases. Link to the scope and architecture documents where appropriate.*  
Work flows of internals, such


[image1]: diagrams/SystemContext.svg
[image2]: diagrams/Containers.svg
[image3]: diagrams/Components_ErasureSet1.svg 