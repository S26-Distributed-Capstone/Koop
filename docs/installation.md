# Installation and Setup

KoopDB uses Docker Compose for orchestrating its services (Query Processors, Storage Nodes, Etcd, Redis).

## Prerequisites

- **Java 21 JDK** (for building the JARs)
- **Maven 3.8+**
- **Docker** and **Docker Compose**
- **AWS CLI** (optional, for interacting with the running cluster)

## Building the Project

1.  Clone the repository:
    ```bash
    git clone https://github.com/S26-Distributed-Capstone/Koop.git
    cd Koop
    ```

2.  Compile and package the project using Maven:
    ```bash
    mvn clean package -DskipTests
    ```
    This will generate the necessary JAR files in `query-processor/target/` and `storage-node/target/`.

## Running the Cluster

1.  Start the cluster using Docker Compose:
    ```bash
    docker-compose up --build
    ```
    This command will build the Docker images for the query processor and storage node services and launch:
    - **6 Storage Nodes** (replicas) on ports `8001-8006` internally mapped to `8080`.
    - **3 Query Processors** (replicas) on ports `9001-9003` internally mapped to `8080`.
    - **Etcd Cluster** (3 nodes, `etcd1`, `etcd2`, `etcd3`) for metadata management.
    - **Kafka** (single broker, KRaft mode, port `9092`) for per-partition commit-message ordering.
    - **Redis** instance for multipart upload session state.
    - **etcd-seeder** (one-shot) which writes the initial `erasure_set_configurations` and `partition_spread_configurations` keys into Etcd.

2.  Verify the services are running:
    ```bash
    docker-compose ps
    ```
    You should see containers for `storage-node`, `query-processor`, `etcd`, and `redis-master` in the `Up` state.

## Service Environment Variables

If you run services outside Docker Compose, you must supply these environment variables.

### Storage Node
| Variable | Description |
| --- | --- |
| `ETCD_URL` | Etcd endpoint (e.g. `http://etcd1:2379`) |
| `REDIS_URL` | Redis URL (e.g. `redis://redis-master:6379`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers (e.g. `kafka:9092`) |
| `NODE_IP` | This node's identity / advertised hostname |

### Query Processor
| Variable | Description |
| --- | --- |
| `APP_PORT` | HTTP listen port (default `8080`) |
| `ETCD_URL` | Etcd endpoint |
| `REDIS_URL` | Redis URL (multipart session state) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers |
| `STORAGE_NODE_URL` | A storage node URL used during initial bootstrap |
| `NODE_IP` | This QP's identity for logging/metrics |

## Erasure Configuration

The erasure-set parameters are seeded into Etcd at startup by the `etcd-seeder` service in `docker-compose.yml`. To change `n`, `k`, `write_quorum`, the participating storage nodes, or the partition spread, edit the `etcd-seeder` `command` block in `docker-compose.yml` and restart the cluster. The seeded keys are:

- `erasure_set_configurations` — list of erasure sets, each with `n`, `k`, `write_quorum`, and the list of `(ip, port)` machines.
- `partition_spread_configurations` — mapping of partitions to erasure sets.

## Stopping the Cluster

To stop the cluster and remove containers/networks:
```bash
docker-compose down
```

To remove volumes (persisted data) as well:
```bash
docker-compose down -v
```

## Accessing the API

The Query Processors expose an S3-compatible API on ports `9001`, `9002`, and `9003`. You can point any S3 client to these endpoints.

**Example Health Check:**
```bash
curl http://localhost:9001/health
# Response: "API Gateway is healthy!"
```

**AWS CLI Configuration:**
To use the AWS CLI with KoopDB, configure a profile or pass endpoints explicitly:
```bash
aws --endpoint-url=http://localhost:9001 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:9001 s3 cp test-file.txt s3://my-bucket/
aws --endpoint-url=http://localhost:9001 s3 ls s3://my-bucket/
```
Note: Authentication is currently disabled, so any credentials will suffice.
