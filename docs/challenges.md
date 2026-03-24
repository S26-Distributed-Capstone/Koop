# Technical Challenges

Building a distributed storage system involves solving several hard problems regarding consistency, availability, and partition tolerance.

## 1. Sequence and Consistency
**Problem:** Ensuring that concurrent writes to the same key result in a deterministic state across multiple nodes.
*Scenario:* Two Query Processors (QPs) receive a `PUT` request for `animals/dog.jpg` simultaneously with different data.
*Challenge:* How do we ensure that all 9 nodes in the erasure set agree on which version is the "latest"?
*Solution:* KoopDB uses a strictly ordered operation log (OpLog). Metadata changes are sequenced (utilizing **Etcd** for cluster-wide configuration and potentially logical clocks or centralized sequencers) to ensure that all nodes apply updates in the same order.

## 2. Distributed Repair
**Problem:** Nodes will inevitably fail or go offline. When a node returns, it is missing data explicitly sent during its downtime.
*Challenge:* How to bring a node back in sync efficiently without stopping the cluster.
*Strategies:*
- **Read Repair:** When a read request fails to find data on a node (but succeeds via quorum), the missing data is reconstructed from parity shards and written back to the lagging node.
- **Active Anti-Entropy:** Background processes (Gossip) that compare Merkle trees or sequence number ranges between nodes to identify and transfer missing data.
- **Full Catch-up:** A node that has been offline for a significant period may need to copy the entire metadata snapshot from a peer before replaying the recent OpLog.

## 3. Metadata Management
**Problem:** The mapping of "Which partition lives on which node?" (Topology) changes over time (e.g., adding nodes).
*Challenge:* Propagating these changes instantly to all Query Processors and Storage Nodes to prevent routing errors.
*Solution:* **Etcd** serves as the source of truth. All components watch Etcd for topology changes. This pushes configuration updates (Erasure Set Configs, Partition Spreads) to all nodes in near real-time.

## 4. Garbage Collection
**Problem:** Deleting an object in a distributed system usually means writing a "tombstone" (delete marker). The actual data takes up space until reclaimed.
*Challenge:* Knowing when it is safe to permanently remove data.
*Solution:* A Gossip-based mechanism propagates the "minimum active sequence number" across the cluster. Once all nodes confirm they have moved past a certain sequence point, older versions and tombstones can be safely purged from RocksDB.
