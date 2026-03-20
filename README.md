# KoopDB

KoopDB is a distributed object storage system designed for high-performance video streaming, offering S3-compatible API endpoints, zero-copy streaming, and strong reliability through erasure coding. This project is an ongoing capstone for the S26 Distributed Systems program.

---

## Overview

KoopDB implements a scalable architecture with the following core components:

- **Query Processor (API Gateway)**: Exposes S3-compatible HTTP endpoints, handles request routing, orchestrates erasure coding, and manages multipart upload state via a cache abstraction layer.
- **Storage Node**: Backend storage servers responsible for persistent shard storage, versioning, and fault tolerance.
- **Common Libraries**: Shared classes for partition placement, replica sets, erasure configuration, pub/sub, and metadata fetching.
- **Cluster Coordination**: Uses etcd for membership/discovery and Redis (planned; stub implemented) as a distributed cache for multipart upload state in multi-QP deployments.

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
   ┌─────┴──────┬───────┐
   ▼            ▼       ▼
 ETCD      Redis      Disk
 Cluster   Cache     Storage
```

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
- **Multipart Upload**: Complete implementation in the Query Processor layer — session state tracked via `CacheClient` abstraction; `MemoryCacheClient` for dev/test, `RedisCacheClient` stub for future multi-QP production use.
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
- **Coordination**: etcd (cluster discovery), Redis (cache — stub implemented, integration pending)

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
- Redis: 6379
- etcd: 2379 (client), 2380 (peer)

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
- Multipart upload lifecycle (initiate/upload/complete/abort) via [`MultipartUploadManager`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/MultipartUploadManager.java)
- Cache abstraction for multipart session state ([`CacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/CacheClient.java), [`MemoryCacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/MemoryCacheClient.java), [`RedisCacheClient`](query-processor/src/main/java/com/github/koop/queryprocessor/processor/cache/RedisCacheClient.java) stub)
- HTTP shard dispatch to storage nodes via `java.net.http.HttpClient` (virtual-thread executor)
- Stripe-based distribution (1 MB shards)

**Main environment variables:**
- `APP_PORT`: HTTP port (default: 8080)
- `ETCD_ENDPOINTS`: etcd endpoints
- `STORAGE_NODE_URL`: storage node URL(s)

### Storage Node

Handles:
- HTTP shard API over Javalin + virtual threads (`PUT/GET/DELETE /store/{partition}/{key}`)
- Disk-based object storage with versioning via RocksDB ([`RocksDbStorageStrategy`](storage-node/src/main/java/com/github/koop/storagenode/db/RocksDbStorageStrategy.java)) or in-memory ([`InMemoryStorageStrategy`](storage-node/src/main/java/com/github/koop/storagenode/db/InMemoryStorageStrategy.java))
- Partition organization and atomic updates with retry logic
- Operation log ([`OpLog`](storage-node/src/main/java/com/github/koop/storagenode/db/OpLog.java)) for durability

**Main environment variables:**
- `PORT`: TCP port (default: 8080)
- `STORAGE_DIR`: Storage path (default: `./storage`)
- `ETCD_ENDPOINTS`, `REDIS_URL`

### Common Libraries

- [`ErasureCoder`](common-lib/src/main/java/com/github/koop/common/erasure/ErasureCoder.java): Reed-Solomon encode/decode
- [`MetadataClient`](common-lib/src/main/java/com/github/koop/common/metadata/MetadataClient.java): cluster metadata with etcd ([`EtcdFetcher`](common-lib/src/main/java/com/github/koop/common/metadata/EtcdFetcher.java)) and in-memory ([`MemoryFetcher`](common-lib/src/main/java/com/github/koop/common/metadata/MemoryFetcher.java)) backends
- [`PubSubClient`](common-lib/src/main/java/com/github/koop/common/pubsub/PubSubClient.java): pub/sub abstraction with [`MemoryPubSub`](common-lib/src/main/java/com/github/koop/common/pubsub/MemoryPubSub.java) implementation
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
java -jar target/query-processor-1-jar-with-dependencies.jar
```

**Storage Node**

```bash
cd storage-node
mvn clean package
java -jar target/storage-node-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

### Directory Structure

```
Koop/
├── docker-compose.yml
├── common-lib/                          # Shared code (erasure, metadata, pub/sub)
│   └── src/main/java/com/github/koop/common/
│       ├── erasure/ErasureCoder.java
│       ├── metadata/                    # MetadataClient, EtcdFetcher, MemoryFetcher
│       └── pubsub/                      # PubSubClient, MemoryPubSub
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
│       │   │       ├── HttpStorageService.java
│       │   │       ├── LocalFileStorage.java
│       │   │       └── TcpStorageService.java
│       │   └── processor/
│       │       ├── StorageWorker.java               # Erasure + node I/O
│       │       ├── ErasureRouting.java
│       │       ├── MultipartUploadManager.java      # Multipart lifecycle
│       │       └── cache/
│       │           ├── CacheClient.java             # Interface (KV + set ops)
│       │           ├── MemoryCacheClient.java       # In-memory (dev/test)
│       │           ├── MultipartUploadSession.java  # Session record + cache keys
│       │           └── RedisCacheClient.java        # Production stub
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
│       │   ├── StorageNodeServer.java
│       │   ├── StorageNode.java
│       │   └── db/                      # RocksDB + in-memory strategies
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
- 1 Redis instance

Expand node count and replica sets by modifying configs in `common-lib/`.

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
