# KoopDB

KoopDB is a distributed object storage system designed for high-performance video streaming, offering S3-compatible API endpoints, zero-copy streaming, and strong reliability through erasure coding. This project is an ongoing capstone for the S26 Distributed Systems program.

---

## Overview

KoopDB implements a scalable architecture with the following core components:

- **Query Processor (API Gateway)**: Exposes S3-compatible HTTP endpoints, handles request routing, orchestrates erasure coding, and manages multipart upload state via a cache abstraction layer.
- **Storage Node**: Backend storage servers responsible for persistent shard storage, versioning, and fault tolerance.
- **Common Libraries**: Shared classes for partition placement, replica sets, erasure configuration, pub/sub, and metadata fetching.
- **Cluster Coordination**: Uses etcd for cluster topology and erasure-set configuration, Redis as the distributed cache for multipart upload session state, and Kafka as the per-partition sequencer / pub-sub bus for ordered commit messages.

The system uses Reed-Solomon erasure coding (default 6 data + 3 parity shards), tolerating up to 3 concurrent node failures.

---

## Architecture

### System Diagram

```
┌──────────────┐
│   Client     │
└──────┬───────┘
       │ HTTP (S3-compatible)
       ▼
┌─────────────────────┐
│  Query Processor    │ (3+ replicas)
│  - REST API         │
│  - Erasure Codec    │
│  - Routing, Hashing │
│  - Multipart Cache  │
└────────┬────────────┘
         │ HTTP (shard store API)
         ▼
┌─────────────────────┐
│   Storage Nodes     │ (9 nodes, 6 data + 3 parity)
│   - Disk I/O        │
│   - Versioning      │
│   - Partitioning    │
└────────┬────────────┘
         │
   ┌─────┼─────────┬────────┬────────┐
   ▼     ▼         ▼        ▼        ▼
 ETCD  Kafka     Redis    Disk     RocksDB
 Cluster (seq.)  (cache)  Storage  (metadata)
```

Kafka carries ordered commit messages (`PutMessage`, `DeleteMessage`, `CreateBucketMessage`, `DeleteBucketMessage`, `MultipartCommitMessage`) from Query Processors to Storage Nodes, providing per-partition sequencing.

Storage nodes expose a simple HTTP shard API (Javalin + virtual threads):
- `PUT /store/{partition}/{key}?requestId=...` — store a shard
- `GET /store/{partition}/{key}` — retrieve a shard
- `DELETE /store/{partition}/{key}` — delete a shard

### Multipart Upload Architecture

```
Client → Main.java (Javalin Gateway)
           └─ StorageWorkerService
                 ├─ putObject/getObject/deleteObject → StorageWorker → Storage Nodes
                 └─ multipart ops → MultipartUploadManager
                                        ├─ CacheClient (interface)
                                        │     ├─ MemoryCacheClient  (dev/test)
                                        │     └─ RedisCacheClient   (production stub)
                                        └─ StorageWorker → Storage Nodes
```

### Design Features

- **S3-Compatible API**: Full PUT, GET, DELETE, HEAD, ListObjectsV2, and multipart upload (initiate/upload/complete/abort).
- **Multipart Upload**: Complete implementation in the Query Processor layer — session state tracked via `CacheClient` abstraction; `MemoryCacheClient` for dev/test, `RedisCacheClient` for production multi-QP deployments.
- **Erasure Coding**: Reed-Solomon encoding (6 data + 3 parity shards; configurable) for data durability and efficiency.
- **Zero-Copy Streaming**: Serves large objects with high throughput using Java 21 virtual threads; shards are streamed directly to storage nodes without buffering.
- **HTTP Shard Transport**: Query Processor communicates with storage nodes over HTTP using `java.net.http.HttpClient` with virtual-thread executor; storage nodes run Javalin.
- **Cache Abstraction**: `CacheClient` interface (KV + set operations) decouples multipart state management from the backing store, enabling a drop-in Redis implementation for distributed deployments.
- **Partition & Erasure Set Configs**: Configurable partition spreading and replica sets via `common-lib`.
- **Dockerized**: Fast startup with Docker Compose for local or production environments.

---

## Technology Stack

- **Language**: Java 21
- **Framework**: Javalin (API Gateway + Storage Nodes)
- **HTTP Client**: `java.net.http.HttpClient` (Query Processor → Storage Node communication)
- **Erasure Coding**: Backblaze Reed-Solomon Java implementation
- **Build**: Maven
- **Containerization**: Docker, Docker Compose
- **Coordination**: etcd (3-node quorum, cluster topology + erasure config), Kafka (per-partition commit-message sequencer), Redis (multipart upload session cache)

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Java 21 JDK (for local or test/dev)
- Maven 3.9+ (for dev builds)

### Quick Start with Docker Compose

```bash
git clone https://github.com/S26-Distributed-Capstone/Koop.git
cd Koop
docker-compose up --build
```

### Verifying Cluster Health

```bash
curl http://localhost:9001/health
# Output: API Gateway is healthy!
```

### Service Ports (default)

- Query Processor HTTP: 9001, 9002, 9003
- Storage Node HTTP: 8001–8009 (9-node erasure set)
- Kafka: 9092
- Redis: 6379
- etcd: 2379 (client), 2380 (peer) — 3-node quorum (`etcd1`, `etcd2`, `etcd3`)

---

## API Reference

### Object Operations

| Method | Path | Operation |
|--------|------|-----------|
| `PUT` | `/{bucket}/{key}` | PutObject |
| `GET` | `/{bucket}/{key}` | GetObject |
| `DELETE` | `/{bucket}/{key}` | DeleteObject |

### Bucket Operations

| Method | Path | Operation |
|--------|------|-----------|
| `PUT` | `/{bucket}` | CreateBucket |
| `DELETE` | `/{bucket}` | DeleteBucket |
| `GET` | `/{bucket}?prefix=P&max-keys=N` | ListObjectsV2 |
| `HEAD` | `/{bucket}` | HeadBucket |

### Multipart Upload

| Method | Path / Query | Operation |
|--------|-------------|-----------|
| `POST` | `/{bucket}/{key}?uploads` | CreateMultipartUpload — returns `uploadId` |
| `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=X` | UploadPart |
| `POST` | `/{bucket}/{key}?uploadId=X` | CompleteMultipartUpload |
| `DELETE` | `/{bucket}/{key}?uploadId=X` | AbortMultipartUpload |

### Other

| Method | Path | Operation |
|--------|------|-----------|
| `GET` | `/health` | Health check |

### Examples

```bash
# Single-object upload
curl -X PUT -T ./video.mp4 http://localhost:9001/videos/movie.mp4

# Retrieve object
curl -O http://localhost:9001/videos/movie.mp4

# Delete object
curl -X DELETE http://localhost:9001/videos/movie.mp4

# Initiate multipart upload
curl -X POST http://localhost:9001/videos/large.mp4?uploads

# Upload a part (uploadId returned from initiate)
curl -X PUT -T ./part1.bin \
  "http://localhost:9001/videos/large.mp4?partNumber=1&uploadId=<uploadId>"

# Complete multipart upload
curl -X POST "http://localhost:9001/videos/large.mp4?uploadId=<uploadId>" \
  -H "Content-Type: application/xml" \
  -d '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>'

# Abort multipart upload
curl -X DELETE "http://localhost:9001/videos/large.mp4?uploadId=<uploadId>"

# List objects in a bucket
curl "http://localhost:9001/videos?prefix=movie&max-keys=100"

# Health check
curl http://localhost:9001/health
```

---

## Components

### Query Processor

Implements:
- S3-compatible REST endpoints (full route map in [`Main.java`](query-processor/src/main/java/com/github/koop/queryprocessor/gateway/Main.java))
- Reed-Solomon erasure coding (encoding/decoding) via [`StorageWorker`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/StorageWorker.java)
- Kafka-sequenced commit publishing (PUT, DELETE, CreateBucket, DeleteBucket, MultipartCommit) via [`CommitCoordinator`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/CommitCoordinator.java)
- Multipart upload lifecycle (initiate/upload/complete/abort) via [`MultipartUploadManager`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/MultipartUploadManager.java)
- Cache abstraction for multipart session state ([`CacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/CacheClient.java), [`MemoryCacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/MemoryCacheClient.java), [`RedisCacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/RedisCacheClient.java))
- HTTP shard dispatch to storage nodes via `java.net.http.HttpClient` (virtual-thread executor)
- Stripe-based distribution (1 MB shards)

**Main environment variables:**
- `APP_PORT`: HTTP port (default: 8080)
- `ETCD_URL`: etcd endpoint (e.g. `http://etcd1:2379`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (e.g. `kafka:9092`)
- `REDIS_URL`: Redis URL (e.g. `redis://redis-master:6379`)
- `STORAGE_NODE_URL`: storage node URL used during initial bootstrap
- `NODE_IP`: this QP's identity for logging/metrics

### Storage Node

Handles:
- HTTP shard API over Javalin + virtual threads (`PUT/GET/DELETE /store/{partition}/{key}`) via [`StorageNodeServerV2`](storage-node/src/main/java/com/github/koop/storagenode/StorageNodeServerV2.java) and [`StorageNodeV2`](storage-node/src/main/java/com/github/koop/storagenode/StorageNodeV2.java)
- Kafka consumer that applies ordered commit messages (PUT / DELETE / bucket / multipart) to RocksDB atomically
- Disk-based shard storage and metadata via the RocksDB-backed [`Database`](storage-node/src/main/java/com/github/koop/storagenode/db/Database.java) (OpLog, Metadata, Bucket tables) and [`RocksDbStorageStrategy`](storage-node/src/main/java/com/github/koop/storagenode/db/RocksDbStorageStrategy.java)
- Operation log ([`OpLog`](storage-node/src/main/java/com/github/koop/storagenode/db/OpLog.java)) for ordering and repair
- Background repair via [`RepairWorkerPool`](storage-node/src/main/java/com/github/koop/storagenode/RepairWorkerPool.java) which detects sequence gaps and replays missed operations from peers

**Main environment variables:**
- `APP_PORT`: HTTP port (default: 8080)
- `ETCD_URL`: etcd endpoint
- `REDIS_URL`: Redis URL
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers
- `NODE_IP`: this node's identity / advertised hostname

### Common Libraries

- [`ErasureCoder`](common-lib/src/main/java/com/github/koop/common/erasure/ErasureCoder.java): Reed-Solomon encode/decode
- [`MetadataClient`](common-lib/src/main/java/com/github/koop/common/metadata/MetadataClient.java): cluster metadata with etcd ([`EtcdFetcher`](common-lib/src/main/java/com/github/koop/common/metadata/EtcdFetcher.java)) and in-memory ([`MemoryFetcher`](common-lib/src/main/java/com/github/koop/common/metadata/MemoryFetcher.java)) backends
- [`PubSubClient`](common-lib/src/main/java/com/github/koop/common/pubsub/PubSubClient.java): pub/sub abstraction with [`KafkaPubSub`](common-lib/src/main/java/com/github/koop/common/pubsub/KafkaPubSub.java) (production) and [`MemoryPubSub`](common-lib/src/main/java/com/github/koop/common/pubsub/MemoryPubSub.java) (dev/test) implementations
- [`ErasureRouting`](common-lib/src/main/java/com/github/koop/common/metadata/ErasureRouting.java): partition → erasure-set resolution
- [`PartitionSpreadConfiguration`](common-lib/src/main/java/com/github/koop/common/metadata/PartitionSpreadConfiguration.java), [`ReplicaSetConfiguration`](common-lib/src/main/java/com/github/koop/common/metadata/ReplicaSetConfiguration.java): cluster topology configuration

---

## Erasure Coding & Cluster Scaling

- **Default setup:** 9 storage nodes (6 data + 3 parity shards), tolerating up to 3 simultaneous node failures.
- **Full deployment:** Configure 9-node erasure sets × 3 for maximum durability and capacity. Adjust with [`ReplicaSetConfiguration`](common-lib/src/main/java/com/github/koop/common/metadata/ReplicaSetConfiguration.java) and [`PartitionSpreadConfiguration`](common-lib/src/main/java/com/github/koop/common/metadata/PartitionSpreadConfiguration.java).
- Cluster can expand/shrink by updating configs and restarting services.

---

## Testing

### Test Suites

| Module | Test Class | Coverage |
|--------|-----------|----------|
| `query-processor` | [`MultipartUploadManagerTest`](query-processor/src/test/java/com/github/koop/queryprocessor/processor/MultipartUploadManagerTest.java) | 12 unit tests — initiate/upload/complete/abort, cache invariants, concurrency |
| `query-processor` | [`MultipartUploadIntegrationTest`](query-processor/src/test/java/com/github/koop/queryprocessor/processor/MultipartUploadIntegrationTest.java) | 5 integration tests — full lifecycle with real `StorageWorker` + `FakeStorageNodeServer` |
| `query-processor` | [`StorageWorkerApiTest`](query-processor/src/test/java/com/github/koop/queryprocessor/processor/StorageWorkerApiTest.java) | StorageWorker put/get/delete against fake nodes |
| `query-processor` | [`S3Test`](query-processor/src/test/java/com/github/koop/queryprocessor/gateway/S3Test.java) | Gateway route tests with mock `StorageService` |
| `storage-node` | [`StorageNodeServerTest`](storage-node/src/test/java/com/github/koop/storagenode/StorageNodeServerTest.java), [`StorageNodeTest`](storage-node/src/test/java/com/github/koop/storagenode/StorageNodeTest.java) | Storage node protocol and logic |
| `system-tests` | [`RealStorageNodesIT`](system-tests/src/test/java/koop/RealStorageNodesIT.java) | End-to-end: 9-node cluster, 15 MB round-trip, 3-node failure tolerance, 4-node failure rejection |

### Running Tests

```bash
# Query Processor unit + integration tests
cd query-processor && mvn test

# Storage Node tests
cd storage-node && mvn test

# Full system integration tests (spins up real 9-node cluster in-process)
cd system-tests && mvn test
```

---

## Development

### Building Locally

**Query Processor**

```bash
cd query-processor
mvn clean package
java -jar target/query-processor-1.0-jar-with-dependencies.jar
```

**Storage Node**

```bash
cd storage-node
mvn clean package
java -jar target/storage-node-1.0-jar-with-dependencies.jar
```

The Docker images use the slim `query-processor-1.0.jar` / `storage-node-1.0.jar` artifacts; the `-jar-with-dependencies` JARs are for running standalone outside Docker.

### Directory Structure

```
Koop/
├── docker-compose.yml
├── common-lib/                          # Shared code (erasure, metadata, pub/sub, messages)
│   └── src/main/java/com/github/koop/common/
│       ├── erasure/                     # ErasureCoder
│       ├── metadata/                    # MetadataClient, EtcdFetcher, MemoryFetcher,
│       │                                #   ErasureRouting, PartitionSpread/ReplicaSet configs
│       ├── messages/                    # Message types (Put/Delete/Bucket/MultipartCommit)
│       └── pubsub/                      # PubSubClient, KafkaPubSub, MemoryPubSub
├── query-processor/                     # API Gateway + Query Processor
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/github/koop/queryprocessor/
│       │   ├── gateway/
│       │   │   ├── Main.java            # Javalin routes (all S3 endpoints)
│       │   │   └── StorageServices/
│       │   │       ├── StorageService.java          # Interface
│       │   │       ├── StorageWorkerService.java    # Production impl
│       │   │       └── LocalFileStorage.java        # Local-only dev impl
│       │   └── processor/
│       │       ├── StorageWorker.java               # Erasure + HTTP shard I/O
│       │       ├── CommitCoordinator.java           # Kafka commit-message publisher
│       │       ├── MultipartUploadManager.java      # Multipart lifecycle
│       │       ├── MultipartUploadResult.java
│       │       └── cache/
│       │           ├── CacheClient.java             # Interface (KV + set ops)
│       │           ├── MemoryCacheClient.java       # In-memory (dev/test)
│       │           ├── MultipartUploadSession.java  # Session record + cache keys
│       │           └── RedisCacheClient.java        # Production Redis backend
│       └── test/
│           ├── gateway/S3Test.java
│           └── processor/
│               ├── FakeStorageNodeServer.java
│               ├── StorageWorkerApiTest.java
│               ├── MultipartUploadManagerTest.java
│               └── MultipartUploadIntegrationTest.java
├── storage-node/                        # Storage backend
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/github/koop/storagenode/
│       │   ├── StorageNodeServerV2.java # Javalin shard API + Kafka consumer wiring
│       │   ├── StorageNodeV2.java       # Shard write/read + commit application
│       │   ├── RepairWorkerPool.java    # Sequence-gap detection + peer replay
│       │   ├── RepairQueue.java
│       │   ├── RepairOperation.java
│       │   ├── BlobRepairStrategy.java
│       │   ├── WriteTracker.java
│       │   └── db/                      # RocksDB-backed Database, OpLog, Metadata, Buckets
│       └── test/
└── system-tests/                        # Full cluster integration tests
    └── src/test/java/koop/
        └── RealStorageNodesIT.java
```

---

## Deployment

Deployed via Docker Compose with service discovery:

- 3 Query Processor replicas (default)
- 9 Storage Node replicas (6 data + 3 parity; expandable with config changes)
- 3-node etcd quorum
- 1 Kafka broker (KRaft mode)
- 1 Redis instance
- 1 one-shot `etcd-seeder` job that writes `erasure_set_configurations` and `partition_spread_configurations` into etcd at startup

Expand node count and replica sets by editing the seeded values in the `etcd-seeder` block of `docker-compose.yml`.

---

## Contributors

Developed as part of the S26 Distributed Systems Capstone.

**Core Team**
- Michael Kupferstein ([@MichaelKupferstein](https://github.com/MichaelKupferstein))
- Yonatan Ginsburg ([@ygins](https://github.com/ygins))
- Eitan Leitner ([@EitanLeit23](https://github.com/EitanLeit23))

---

## Project Status

**Active development** – KoopDB is being actively built and maintained as a demonstration of advanced distributed storage concepts. Expect rapid changes and ongoing improvements as capstone features are completed.
