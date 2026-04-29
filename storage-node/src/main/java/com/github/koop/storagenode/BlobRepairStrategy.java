package com.github.koop.storagenode;

/**
 * Strategy for executing a single repair operation. Implementations are
 * responsible for fetching shards from peer nodes and writing the repaired
 * blob to local disk.
 *
 * <p>Injected into {@link RepairWorkerPool} at construction time so that
 * the pool has no direct dependency on {@link StorageNodeV2}.
 */
@FunctionalInterface
public interface BlobRepairStrategy {
    void repair(RepairOperation operation);
}
