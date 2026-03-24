# Technologies & Dependencies

KoopDB leverages a modern Java stack focused on high-performance concurrency and distributed systems primitives.

## Core Language
- **Java 21 (LTS):** Utilizing the latest language features, particularly **Virtual Threads (Project Loom)** for handling massive concurrency in the Query Processors and Storage Nodes without reactive complexity.

## Key Libraries

### Web Framework
- **Javalin (7.0.1):** A lightweight, unopinionated web framework for Java/Kotlin.
  - Used for the REST API (S3 routes) on Query Processors.
  - Used for internal node-to-node communication APIs on Storage Nodes.
  - Configured to use Virtual Threads for optimal throughput.

### Storage Engine
- **RocksDB (8.10.0):** An embeddable persistent key-value store for fast storage.
  - Used by Storage Nodes to persist object metadata, data shards, and operation logs on disk.
  - Efficient for high write throughput (LSM-Tree based).

### Serialization & Logging
- **Jackson (2.17.0):** JSON serialization/deserialization for API responses and internal messages.
- **Log4j 2 (2.23.1):** Asynchronous logging framework.

### Distributed Coordination
- **Etcd (via Jetcd):** Distributed key-value store for shared configuration and service discovery.
- **Redis (via Lettuce/Jedis):** In-memory data structures for caching multipart upload state and sequencing.

### Testing & Verification
- **JUnit 5:** Unit and integration testing.
- **Testcontainers:** For spinning up Etcd, Redis, and MinIO/S3 mock containers during integration tests.
- **AWS SDK for Java (2.x):** Used in system tests to verify S3 compatibility against the running cluster.

## Infrastructure
- **Docker & Docker Compose:** Containerization of all components for consistent development and deployment environments.
- **Maven:** Build automation and dependency management.
