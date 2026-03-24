workspace "Distributed Object Store" "An S3-compatible distributed object store using erasure encoding for redundancy." {

    model {

        # ── External Actors ──────────────────────────────────────────────
        client = person "Application Client" "Any application that talks to the store via the standard S3 HTTP API (PUT, GET, DELETE, multipart upload, bucket ops)."

        # ── Software System ───────────────────────────────────────────────
        objectStore = softwareSystem "Distributed Object Store" "S3-compatible pluggable backend. Objects are split into shards and distributed across erasure sets so the system can tolerate losing up to k nodes per set." {

            # ── Containers (Level 2) ─────────────────────────────────────

            queryProcessor = container "Query Processor (QP)" "Stateless request router. Accepts S3 API calls, streams object data to storage nodes, publishes multipart commit messages to the sequester, and waits for quorum ACKs before replying to the client." "Go / HTTP" "Service"

            sequencer = container "Sequencer" "Pub/Sub system (e.g. Kafka) used for multipart upload commit sequencing. Planned to sequence all mutations (PUT/DELETE) in the future." "Kafka / PubSub" "Message Broker"

            etcd = container "etcd" "Distributed configuration store. Holds the partition-spread map (partition → erasure set) and the erasure-set map (erasure set → machine IPs). Read by QP on startup and on config change." "etcd" "DataStore"

            cache = container "Multipart Upload Cache" "Tracks in-flight multipart upload sessions: upload ID → object key, parts uploaded so far. Checked on Upload-Part and Complete-Multipart-Upload to validate completeness before committing." "Redis / in-memory store" "DataStore"

            erasureSet1 = container "Erasure Set 1" "Logical group of 9 storage nodes. Objects hashed to partitions 0–8 are sharded across these nodes. Can tolerate 3 simultaneous node failures." "Erasure Set" "Cluster" {

                storageNode1  = component "Storage Node 1"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode2  = component "Storage Node 2"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode3  = component "Storage Node 3"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode4  = component "Storage Node 4"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode5  = component "Storage Node 5"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode6  = component "Storage Node 6"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode7  = component "Storage Node 7"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode8  = component "Storage Node 8"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"
                storageNode9  = component "Storage Node 9"  "Stores one erasure shard per object (partition_{p}/{key}/{id}). V2/Planned: repairs & RocksDB metadata." "Go / HTTP"

                rocksDB1 = component "RocksDB Instance" "(V2/Planned) Embedded KV store on each node. Contains three column families: (1) Operation Log — seq# → (key, op); (2) Metadata — key → (seq#, shard path / multipart keys); (3) Buckets — bucket → (partition, seq#). All related writes are committed atomically." "RocksDB"
            }

            erasureSet2 = container "Erasure Set 2" "Logical group of 9 storage nodes. Objects hashed to partitions 9–17 land here. Can tolerate 3 simultaneous node failures." "Erasure Set" "Cluster"

            erasureSet3 = container "Erasure Set 3" "Logical group of 9 storage nodes. Objects hashed to partitions 18–26 land here. Can tolerate 3 simultaneous node failures." "Erasure Set" "Cluster"
        }

        # ── Relationships – Level 1 (Context) ────────────────────────────
        client      -> objectStore "Sends S3 API requests (PUT, GET, DELETE, bucket ops, multipart)" "HTTPS / S3 protocol"
        objectStore -> client      "Returns S3 responses (data, ACKs, error codes)"

        # ── Relationships – Level 2 (Container) ──────────────────────────

        # QP ↔ external config
        queryProcessor -> etcd         "Reads partition-spread & erasure-set config on startup / config change" "gRPC"

        # QP ↔ sequencer
        queryProcessor -> sequencer    "Publishes multipart commit messages (planned: all mutations)" "Pub/Sub protocol"

        # QP ↔ storage nodes (data plane)
        queryProcessor -> erasureSet1  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs" "HTTP/gRPC"
        queryProcessor -> erasureSet2  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs" "HTTP/gRPC"
        queryProcessor -> erasureSet3  "Streams erasure-encoded shards; sends GET requests; waits for quorum ACKs" "HTTP/gRPC"

        # Sequencer → storage nodes (control plane)
        sequencer -> erasureSet1       "Delivers multipart commit messages (seq# + key + parts); nodes subscribe per erasure set" "Pub/Sub"
        sequencer -> erasureSet2       "Delivers multipart commit messages" "Pub/Sub"
        sequencer -> erasureSet3       "Delivers multipart commit messages" "Pub/Sub"

        # Storage nodes ↔ each other (repair)
        erasureSet1 -> erasureSet1     "(V2/Planned) Peer-to-peer repair broadcast: node requests missing ops (seq gaps) from siblings" "HTTP/gRPC"

        # QP ↔ multipart cache
        queryProcessor -> cache        "Creates upload session; records uploaded parts; validates completeness on Complete" "Redis protocol"

        # ACKs back to QP
        erasureSet1 -> queryProcessor  "ACKs after atomically writing shard (V2: + RocksDB metadata)" "HTTP/gRPC"
        erasureSet2 -> queryProcessor  "ACKs after atomically writing shard (V2: + RocksDB metadata)" "HTTP/gRPC"
        erasureSet3 -> queryProcessor  "ACKs after atomically writing shard (V2: + RocksDB metadata)" "HTTP/gRPC"

        # ── Relationships – Level 3 (Component, ES1 internals) ───────────
        storageNode1 -> rocksDB1 "(V2/Planned) Atomically writes (op-log entry + metadata entry)" "RocksDB API"
        storageNode2 -> rocksDB1 "(V2/Planned) Atomically writes shard metadata" "RocksDB API"
        storageNode3 -> rocksDB1 "(V2/Planned) Atomically writes shard metadata" "RocksDB API"

        # Repair path (representative)
        storageNode1 -> storageNode2 "(V2/Planned) Broadcasts repair request for missed seq# range" "HTTP/gRPC"
        storageNode1 -> storageNode3 "(V2/Planned) Broadcasts repair request for missed seq# range" "HTTP/gRPC"
    }

    # ── Views ─────────────────────────────────────────────────────────────
    views {

        # Level 1 – System Context
        systemContext objectStore "SystemContext" {
            include *
            autoLayout lr
            title "Level 1 – System Context: Distributed Object Store"
            description "The application client calls the store using the standard S3 API. Internally the store routes, sequences, and durably shards every object."
        }

        # Level 2 – Containers
        container objectStore "Containers" {
            include *
            autoLayout lr
            title "Level 2 – Containers: Distributed Object Store"
            description "Shows the major runtime processes and data stores. The Query Processor is the S3 entry point. The Sequencer handles multipart commits. Storage nodes persist shards. etcd holds cluster configuration."
        }

        # Level 3 – Components (zoom into Erasure Set 1)
        component erasureSet1 "Components_ErasureSet1" {
            include *
            autoLayout tb
            title "Level 3 – Components: Erasure Set 1"
            description "Inside a single erasure set. Each of the 9 storage nodes holds one shard per object (partition_{p}/{key}/{id}) and (Planned/V2) its own RocksDB instance."
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
