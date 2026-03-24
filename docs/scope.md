# Project Scope

## In Scope

The KoopDB project focuses on building a **distributed, fault-tolerant object storage system** compatible with a subset of the Amazon S3 API. The primary goals are to demonstrate advanced distributed systems concepts such as erasure coding, consensus-based metadata management, and high availability.

### Core Features
- **S3 Compatibility:** Support for standard S3 tools (like AWS CLI and SDKs) for fundamental operations.
- **Distributed Architecture:** A headless cluster of Query Processors (gateways) and Storage Nodes.
- **Fault Tolerance:** Data is sharded using **Erasure Coding** (Reed-Solomon equivalent), allowing the system to survive node failures without data loss.
- **Metadata Management:** Centralized, consistent metadata storage using **Etcd** to manage partition maps and ring topology.
- **Multipart Uploads:** Support for uploading large objects in parts, reassembled upon completion.

### supported Operations
- **Buckets:** Create, Delete, List, Head.
- **Objects:** Put, Get, Delete.
- **Multipart:** Initiate, Upload Part, Complete, Abort.

## Out of Scope

While KoopDB mimics S3, it is not a full replacement. The following features are explicitly out of scope for this version:

- **Authentication/Authorization:** No IAM, ACLs, or signed URL validation (access is open/anonymous).
- **Advanced S3 Features:** Object Versioning, Lifecycle Policies, Server-Side Encryption, Object Tagging, BitTorrent support.
- **Strong Consistency for List Operations:** While data operations aim for consistency, listing might be eventually consistent depending on the metadata cache state.
- **HTTPS/TLS:** Traffic is currently HTTP-only.
