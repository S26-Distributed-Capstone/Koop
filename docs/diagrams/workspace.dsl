workspace "Distributed Object Store" "An S3-compatible distributed object store using erasure encoding for redundancy." {

    model {

        # ── External Actors ──────────────────────────────────────────────
        client = person "Application Client" "Any application that talks to the store via the standard S3 HTTP API (PUT, GET, DELETE, multipart upload, bucket ops)."

        # ── Software System ───────────────────────────────────────────────
        objectStore = softwareSystem "Distributed Object Store" "S3-compatible pluggable backend. Objects are split into shards and distributed across erasure sets so the system can tolerate losing up to k nodes per set." {

            # ── Containers (Level 2) ─────────────────────────────────────

            queryProcessor = container "Query Processor (QP)" "Stateless request router. Accepts S3 API calls, streams object data to storage nodes, publishes ordered commit messages to the sequencer, and waits for quorum ACKs before replying to the client. Maintains a local discovery cache (NodeHealthTracker) refreshed by a background NodeHealthProbe (GET /health every 5s) to skip nodes known to be down without changing the quorum denominator." "Java 21 / HTTP" "Service"

            sequencer = container "Sequencer" "Pub/Sub system (Kafka, KRaft mode) used for per-partition ordering of all mutating operations (PutMessage, DeleteMessage, CreateBucketMessage, DeleteBucketMessage, MultipartCommitMessage) and for gossip-cluster watermark dissemination used by garbage collection." "Kafka / PubSub" "Message Broker"

            etcd = container "etcd" "Distributed configuration store (3-node quorum). Holds the partition-spread map (partition → erasure set) and the erasure-set map (erasure set → machine IPs). Read by QP on startup and watched for changes." "etcd" "DataStore"

            cache = container "Multipart Upload Cache" "Tracks in-flight multipart upload sessions: upload ID → object key, parts uploaded so far, cached part sizes. Checked on Upload-Part and Complete-Multipart-Upload to validate completeness before committing." "Redis / in-memory store" "DataStore"

            erasureSet1 = container "Erasure Set 1" "Logical group of 9 storage nodes (configurable; docker-compose uses 6). Objects hashed to partitions 0–8 are sharded across these nodes. Tolerates up to k = n − m simultaneous node failures (default m=6 data + k=3 parity in system tests; m=4 data + k=2 parity in docker-compose)." "Erasure Set" "Cluster" {

                storageNode1  = component "Storage Node 1"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). Each storage node runs the Kafka commit consumer, the GossipService, the GarbageCollectorWorker, and the BlobDeletionWorker." "Java 21 / HTTP"
                storageNode2  = component "Storage Node 2"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode3  = component "Storage Node 3"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode4  = component "Storage Node 4"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode5  = component "Storage Node 5"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode6  = component "Storage Node 6"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode7  = component "Storage Node 7"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode8  = component "Storage Node 8"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"
                storageNode9  = component "Storage Node 9"  "Stores one erasure shard per object (partition_{p}/{key}/{id})." "Java 21 / HTTP"

                gossipService = component "Gossip Service" "Runs on every storage node. Periodically broadcasts watermark messages (last applied sequence number per partition, lowest sequence number of any in-flight GET) to the gossip-cluster topic so peers can compute the cluster-wide safe-to-GC watermark." "Java 21 / Kafka"

                gcWorker = component "Garbage Collector Worker" "Runs on every storage node. Consumes gossip-derived watermarks and scans the RocksDB op_log / metadata / gc_cursors column families for records below the cluster watermark, then schedules obsolete shards for deletion." "Java 21"

                blobDeleter = component "Blob Deletion Worker" "Runs on every storage node. Drains the pending_deletes column family asynchronously and physically removes shard files from disk, decoupling client DELETE latency from on-disk cleanup." "Java 21"

                rocksDB1 = component "RocksDB Instance" "Embedded KV store on each storage node. Column families: op_log (seq# → (key, op)), metadata (key → (seq#, shard path / multipart keys, size)), buckets (bucket → (partition, seq#, deleted)), uncommitted_writes, repair_queue, pending_deletes (drained by BlobDeletionWorker), gc_cursors. PUT / DELETE / bucket ops commit related writes atomically across families." "RocksDB"
            }

            erasureSet2 = container "Erasure Set 2" "Logical group of 9 storage nodes. Objects hashed to partitions 9–17 land here. Tolerates 3 simultaneous node failures (m=6 data + k=3 parity)." "Erasure Set" "Cluster"

            erasureSet3 = container "Erasure Set 3" "Logical group of 9 storage nodes. Objects hashed to partitions 18–26 land here. Tolerates 3 simultaneous node failures (m=6 data + k=3 parity)." "Erasure Set" "Cluster"
        }

        # ── Relationships – Level 1 (Context) ────────────────────────────
        client      -> objectStore "Sends S3 API requests (PUT, GET, DELETE, bucket ops, multipart)" "HTTPS / S3 protocol"
        objectStore -> client      "Returns S3 responses (data, ACKs, error codes)"

        # ── Relationships – Level 2 (Container) ──────────────────────────

        # QP ↔ external config
        queryProcessor -> etcd         "Reads partition-spread & erasure-set config on startup; watches for changes" "gRPC"

        # QP ↔ sequencer
        queryProcessor -> sequencer    "Publishes ordered commit messages (PUT / DELETE / CreateBucket / DeleteBucket / MultipartCommit) to partition-keyed topics" "Pub/Sub protocol"

        # QP ↔ storage nodes (data plane)
        queryProcessor -> erasureSet1  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs; probes GET /health every 5s (NodeHealthProbe → NodeHealthTracker)" "HTTP"
        queryProcessor -> erasureSet2  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs; probes GET /health every 5s" "HTTP"
        queryProcessor -> erasureSet3  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs; probes GET /health every 5s" "HTTP"

        # Sequencer → storage nodes (control plane: commits + gossip)
        sequencer -> erasureSet1       "Delivers per-partition commit messages (PUT / DELETE / bucket / multipart) and gossip-cluster watermarks; nodes subscribe per erasure set" "Pub/Sub"
        sequencer -> erasureSet2       "Delivers per-partition commit messages and gossip-cluster watermarks" "Pub/Sub"
        sequencer -> erasureSet3       "Delivers per-partition commit messages and gossip-cluster watermarks" "Pub/Sub"

        # Storage nodes ↔ each other (repair + gossip)
        erasureSet1 -> erasureSet1     "Peer-to-peer repair broadcast: node requests missing ops (seq gaps) from siblings; also publishes watermark gossip on the gossip-cluster topic so the cluster can converge on a safe GC cursor" "HTTP + Pub/Sub"

        # QP ↔ multipart cache
        queryProcessor -> cache        "Creates upload session; atomically reserves part numbers; records part sizes; validates completeness on Complete; cleans up on Abort" "Redis protocol"

        # ACKs back to QP
        erasureSet1 -> queryProcessor  "ACKs after atomically writing shard + RocksDB metadata; responds 200 to health probes" "HTTP"
        erasureSet2 -> queryProcessor  "ACKs after atomically writing shard + RocksDB metadata; responds 200 to health probes" "HTTP"
        erasureSet3 -> queryProcessor  "ACKs after atomically writing shard + RocksDB metadata; responds 200 to health probes" "HTTP"

        # ── Relationships – Level 3 (Component, ES1 internals) ───────────
        storageNode1 -> rocksDB1 "Atomically writes op_log + metadata + (optionally) bucket + pending_deletes entries via a single WriteBatch" "RocksDB API"
        storageNode2 -> rocksDB1 "Atomically writes shard metadata across column families" "RocksDB API"
        storageNode3 -> rocksDB1 "Atomically writes shard metadata across column families" "RocksDB API"

        # Repair path (representative)
        storageNode1 -> storageNode2 "Broadcasts repair request for missed seq# range" "HTTP"
        storageNode1 -> storageNode3 "Broadcasts repair request for missed seq# range" "HTTP"

        # Gossip / GC / blob-delete component wiring (representative — same pattern on every node)
        storageNode1 -> gossipService "Hosts the gossip publisher/subscriber loop" "in-process"
        storageNode1 -> gcWorker      "Hosts the GC scan loop" "in-process"
        storageNode1 -> blobDeleter   "Hosts the async pending_deletes drainer" "in-process"
        gossipService -> sequencer    "Publishes watermark messages to the gossip-cluster topic; subscribes to peer watermarks" "Pub/Sub"
        gcWorker      -> rocksDB1     "Reads op_log + metadata + gc_cursors; writes pending_deletes for shards below the cluster watermark" "RocksDB API"
        blobDeleter   -> rocksDB1     "Drains pending_deletes; deletes shard files from disk" "RocksDB API"
    }

    # ── Views ─────────────────────────────────────────────────────────────
    views {

        # Level 1 – System Context
        systemContext objectStore "SystemContext" {
            include *
            autoLayout lr
            title "Level 1 – System Context: Distributed Object Store"
            description "The application client calls the store using the standard S3 API. Internally the store routes, sequences, durably shards every object, gossips watermarks, and asynchronously garbage-collects obsolete shards."
        }

        # Level 2 – Containers
        container objectStore "Containers" {
            include *
            autoLayout lr
            title "Level 2 – Containers: Distributed Object Store"
            description "Major runtime processes and data stores. QP is the S3 entry point and runs the discovery-cache probe. Sequencer carries commit messages and gossip watermarks. Storage nodes persist shards. etcd holds cluster configuration. Multipart cache tracks in-flight uploads."
        }

        # Level 3 – Components (zoom into Erasure Set 1)
        component erasureSet1 "Components_ErasureSet1" {
            include *
            autoLayout tb
            title "Level 3 – Components: Erasure Set 1"
            description "Inside a single erasure set. Each of the 9 storage nodes holds one shard per object (partition_{p}/{key}/{id}) and runs the GossipService, GarbageCollectorWorker, and BlobDeletionWorker against its own RocksDB instance."
        }

        # ── Styles ────────────────────────────────────────────────────────
        styles {
            element "Person" {
                shape Person
                background #1168bd
                color #ffffff
            }
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Component" {
                background #85bbf0
                color #000000
            }
            element "DataStore" {
                shape Cylinder
                background #f5a623
                color #000000
            }
            element "Cluster" {
                shape RoundedBox
                background #2d6a4f
                color #ffffff
            }
            relationship "* " {
                thickness 2
            }
        }

        theme default
    }
}
