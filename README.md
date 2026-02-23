# KoopDB

KoopDB is a distributed object storage system designed for high-performance video streaming, offering S3-compatible API endpoints, zero-copy streaming, and strong reliability through erasure coding. This project is an ongoing capstone for the S26 Distributed Systems program.

---

## Overview

KoopDB implements a scalable architecture with the following core components:

- **Query Processor (API Gateway)**: Exposes RESTful S3-like HTTP interfaces, handles request hashing, routes data, and orchestrates erasure coding and streaming.
- **Storage Node**: Backend storage servers running a custom binary protocol, responsible for persistent storage, partitioning, versioning, and fault tolerance.
- **Common Libraries**: Shared classes and configurations for partition placement, replica sets, and protocol definitions, facilitating cluster expansion.
- **Cluster Coordination**: Uses etcd (for membership/discovery) and Redis (as a low-latency cache).

The system is engineered around Reed-Solomon erasure coding (default 6 data + 3 parity shards) for strong fault tolerance, supporting up to 3 concurrent node failures.

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
└────────┬────────────┘
         │ Binary Protocol (TCP)
         ▼
┌─────────────────────┐
│   Storage Nodes     │ (6+ replicas, 3 erasure sets possible)
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

### Design Features

- **S3-Compatible API**: PUT, GET, DELETE with standard error responses.
- **Erasure Coding**: Reed-Solomon encoding (6 data + 3 parity shards; configurable) for data durability and efficiency.
- **Partition & Erasure Set Configs**: Uses configurable partition spreading and replica sets (`common-lib`) for sharding and cluster expansion.
- **Zero-Copy Streaming**: Serves large objects with high throughput (Java 21 virtual threads).
- **Service Discovery & Orchestration**: Automatic discovery, scaling friendly.
- **Dockerized**: Fast startup with Docker Compose for local or production environments.

---

## Technology Stack

- **Language**: Java 21
- **Framework**: Javalin (API Gateway/Query Processor)
- **Erasure Coding**: Backblaze Reed-Solomon Java implementation
- **Build**: Maven
- **Containerization**: Docker, Docker Compose
- **Coordination**: etcd (cluster discovery), Redis (cache)

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
- Storage Node TCP: 8001–8006 (expandable)
- Redis: 6379
- etcd: 2379 (client), 2380 (peer)

---

## API Usage

| Operation     | Example                                                           |
|---------------|-------------------------------------------------------------------|
| PUT           | `curl -X PUT -T ./video.mp4 http://localhost:9001/videos/movie.mp4` |
| GET           | `curl -O http://localhost:9001/videos/movie.mp4`                  |
| DELETE        | `curl -X DELETE http://localhost:9001/videos/movie.mp4`           |
| Health Check  | `curl http://localhost:9001/health`                               |

See `query-processor/README.md` for more endpoint and protocol details.

---

## Components

### Query Processor

Implements:
- S3-compatible REST endpoints (PUT/GET/DELETE)
- Reed-Solomon erasure coding (encoding/decoding)
- Consistent hashing and smart routing
- Protocol bridging (HTTP REST ↔ Custom Binary TCP)
- Stripe-based distribution (1 MB shards)

**Main environment variables:**
- `APP_PORT`: HTTP port (default: 8080)
- `ETCD_ENDPOINTS`: etcd endpoints
- `STORAGE_NODE_URL`: storage node URL(s)

### Storage Node

Handles:
- Custom binary protocol over TCP (length-prefixed frames, opcodes)
- Disk-based object storage with versioning
- Partition organization & atomic updates (with retry logic)
- Periodic garbage collection

**Main environment variables:**
- `PORT`: TCP port (default: 8080)
- `STORAGE_DIR`: Storage path (default: ./storage)
- `ETCD_ENDPOINTS`, `REDIS_URL`

### Common Libraries

- Partition/erasure set configs—expand cluster or rebalance with configs (`common-lib`).
- Modern objects for partition/replica spread and configuration management.

---

## Erasure Coding & Cluster Scaling

- **Default setup:** 6 storage nodes with striping.
- **Full deployment:** Configure 9-node erasure sets × 3 for maximum durability and capacity. Easily adjust with `ReplicaSetConfiguration` and `PartitionSpreadConfiguration`.
- Cluster can expand/shrink by updating configs and restarting services.

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

### Running Tests

```bash
cd query-processor && mvn test
cd storage-node && mvn test
```

### Directory Structure

```
Koop/
├── docker-compose.yml           # Orchestration config
├── common-lib/                  # Shared code (partition, replica, config)
│   └── src/main/java/com/github/koop/common/
├── query-processor/             # API Gateway/Query Processor
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/github/koop/queryprocessor/
│       └── test/
├── storage-node/                # Storage backend
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/github/koop/storagenode/
│       └── test/
```

---

## Deployment

Deployed via Docker Compose with service discovery:

- 3 Query Processor replicas (default)
- 6 Storage Node replicas (expandable up to 27 with config changes)
- 3-node etcd quorum
- 1 Redis instance

You may expand node count and replica sets by modifying configs in `common-lib/`.

---

## Contributors

Developed as part of the S26 Distributed Systems Capstone.

**Core Team**
- Michael Kupferstein ([@MichaelKupferstein](https://github.com/MichaelKupferstein))
- Yonatan Ginsburg ([@ygins](https://github.com/ygins))
- Eitan Leitner ([@EitanLeit23](https://github.com/EitanLeit23))

---

## License

[Project license to be added or specified.]

---

## Project Status

**Active development** – KoopDB is being actively built and maintained as a demonstration of advanced distributed storage concepts. Expect rapid changes and ongoing improvements as capstone features are completed.
