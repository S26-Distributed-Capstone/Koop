package com.github.koop.storagenode;

/**
 * Abstraction for enqueuing repair operations, decoupling producers
 * ({@link StorageNodeV2}) from the repair scheduler ({@link RepairWorkerPool}).
 */
@FunctionalInterface
public interface RepairQueue {
    void enqueue(RepairOperation operation);
}
