# KoopDB

KoopDB is a distributed object storage system designed for high-performance video streaming applications. It provides S3-compatible API endpoints with erasure coding for reliability and efficient storage.

## Overview

KoopDB implements a distributed storage architecture with two main components:
- **Query Processor**: API Gateway providing RESTful HTTP interface for object storage operations
- **Storage Node**: Backend storage server handling binary protocol and disk persistence

The system uses Reed-Solomon erasure coding (6+3 configuration) to provide fault tolerance, allowing up to 3 node failures while maintaining data availability.

## Architecture

### System Components

```
┌──────────────┐
│   Client     │
└──────┬───────┘
       │ HTTP (S3-compatible)
       ▼
┌──────────────────┐
│ Query Processor  │ (3 replicas)
│  - API Gateway   │
│  - Erasure Codec │
│  - Load Balancer │
└────────┬─────────┘
         │ Custom Binary Protocol (TCP)
         ▼
┌──────────────────┐
│  Storage Nodes   │ (6 nodes in 3 sets)
│  - Disk I/O      │
│  - Versioning    │
│  - Partitioning  │
└────────┬─────────┘
         │
    ┌────┴────┬────────┐
    ▼         ▼        ▼
  ETCD     Redis    Storage
(Cluster)  (Cache)    (Disk)
```

### Key Features

- **S3-Compatible API**: Standard PUT, GET, DELETE operations with S3-style XML error responses
- **Erasure Coding**: Reed-Solomon (6,3) encoding for data redundancy and fault tolerance
- **Zero-Copy Streaming**: Efficient data transfer without buffering entire files in memory
- **Virtual Threads**: Java 21 virtual threads for high concurrency
- **Consistent Hashing**: Automatic key distribution across storage sets using CRC32
- **Versioning**: Atomic updates with version tracking for concurrent access safety
- **Docker-Ready**: Multi-stage Dockerfiles and Docker Compose orchestration

## Technology Stack

- **Language**: Java 21
- **Web Framework**: Javalin (Query Processor)
- **Erasure Coding**: Backblaze Reed-Solomon
- **Orchestration**: Docker Compose
- **Service Discovery**: etcd (3-node cluster)
- **Caching**: Redis
- **Build Tool**: Maven

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Java 21 JDK (for local development)
- Maven 3.9+ (for local development)

### Quick Start with Docker Compose

1. **Clone the repository**:
   ```bash
   git clone https://github.com/S26-Distributed-Capstone/Koop.git
   cd Koop
   ```

2. **Start the cluster**:
   ```bash
   docker-compose up --build
   ```

3. **Verify services are running**:
   ```bash
   curl http://localhost:9001/health
   # Output: API Gateway is healthy!
   ```

### Service Ports

- **Query Processors**: 9001, 9002, 9003 (HTTP)
- **Storage Nodes**: 8001-8006 (TCP Binary Protocol)
- **Redis**: 6379
- **etcd**: 2379 (client), 2380 (peer)

## API Usage

KoopDB provides an S3-compatible REST API for object storage operations.

### Store an Object (PUT)

```bash
curl -X PUT -T ./video.mp4 http://localhost:9001/videos/movie.mp4
```

### Retrieve an Object (GET)

```bash
curl -O http://localhost:9001/videos/movie.mp4
```

### Delete an Object (DELETE)

```bash
curl -X DELETE http://localhost:9001/videos/movie.mp4
```

### Health Check

```bash
curl http://localhost:9001/health
```

## Components

### Query Processor

The Query Processor serves as the API Gateway and implements:
- RESTful HTTP endpoints (S3-compatible)
- Erasure coding/decoding using Reed-Solomon algorithm
- Request routing based on consistent hashing (CRC32)
- Protocol translation (HTTP → Binary TCP)
- Stripe-based data distribution (1MB shard size)

**Configuration** (Environment Variables):
- `APP_PORT`: HTTP port (default: 8080)
- `ETCD_ENDPOINTS`: etcd cluster addresses
- `STORAGE_NODE_URL`: Storage node service URL

### Storage Node

The Storage Node handles:
- Binary protocol over TCP (custom framing)
- Disk-based object storage with versioning
- Partition-based organization
- Atomic version updates with retry logic
- Asynchronous garbage collection

**Configuration** (Environment Variables):
- `PORT`: TCP port (default: 8080)
- `STORAGE_DIR`: Data directory path (default: ./storage)
- `ETCD_ENDPOINTS`: etcd cluster addresses
- `REDIS_URL`: Redis connection URL

### Storage Protocol

The binary protocol uses 8-byte length-prefixed frames:

| Component       | Size      | Description                    |
|-----------------|-----------|--------------------------------|
| Frame Length    | 8 bytes   | Total payload + opcode length  |
| Opcode          | 4 bytes   | 1=PUT, 2=DELETE, 6=GET        |
| Payload         | Variable  | Operation-specific data        |

## Erasure Coding

KoopDB uses Reed-Solomon erasure coding with parameters:
- **K (Data Shards)**: 6
- **M (Parity Shards)**: 3
- **Total Shards**: 9
- **Shard Size**: 1 MB

This configuration tolerates up to 3 simultaneous node failures. Data is striped across shards with each stripe containing 6 MB of real data plus 3 MB of parity data.

### Routing Strategy

Objects are distributed across 3 erasure sets using consistent hashing:
```
hash(key) % 100:
  0-33   → Set 1
  34-66  → Set 2
  67-99  → Set 3
```

Each set contains 9 storage nodes (6 data + 3 parity).

## Development

### Building Locally

**Query Processor**:
```bash
cd query-processor
mvn clean package
java -jar target/query-processor-1.jar
```

**Storage Node**:
```bash
cd storage-node
mvn clean package
java -jar target/storage-node-1.0.0-SNAPSHOT.jar
```

### Running Tests

```bash
# Query Processor tests
cd query-processor
mvn test

# Storage Node tests
cd storage-node
mvn test
```

### Directory Structure

```
Koop/
├── docker-compose.yml        # Orchestration config
├── query-processor/          # API Gateway component
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/github/koop/queryprocessor/
│       │   ├── gateway/      # HTTP endpoints
│       │   └── processor/    # Erasure coding logic
│       └── test/
└── storage-node/             # Storage backend component
    ├── Dockerfile
    ├── pom.xml
    └── src/
        ├── main/java/com/github/koop/storagenode/
        │   ├── StorageNode.java       # Core storage logic
        │   ├── StorageNodeServer.java # TCP server
        │   └── Handler.java           # Protocol handlers
        └── test/
```

## Deployment

The system deploys 12 containers via Docker Compose:
- 3 Query Processor replicas
- 6 Storage Node replicas
- 3 etcd nodes (cluster quorum)
- 1 Redis instance

All containers are networked and configured to discover each other via etcd.

## Contributing

This project is part of the S26 Distributed Systems Capstone. For contributions:
1. Create a feature branch
2. Make your changes
3. Submit a pull request with reviewers

## License

[Add license information]

## Project Status

Active development - This is an ongoing distributed systems capstone project focused on building a scalable object storage system for video streaming applications.
