package com.github.koop.storagenode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages a pool of virtual-thread workers that process {@link RepairOperation}s
 * with debounced, last-writer-wins compaction.
 *
 * <p>When the same key is enqueued multiple times before execution, only the
 * operation with the highest {@link RepairOperation#seqOffset()} is retained.
 * An incoming operation whose seqOffset is less than or equal to the pending
 * operation's seqOffset is silently discarded (at-least-once idempotency).
 *
 * <p>A short delay between enqueue and execution lets rapid sequential updates
 * compact and gives in-progress writes time to finish before a repair fires.
 */
public class RepairWorkerPool {

    private static final Logger logger = LogManager.getLogger(RepairWorkerPool.class);

    static final long REPAIR_DELAY_MS = 2_000;

    private final ConcurrentHashMap<String, RepairOperation> pendingOperations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> scheduledFutures = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("repair-scheduler").factory());
    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
    private final WriteTracker writeTracker;
    private final long repairDelayMs;
    private volatile boolean running;

    public RepairWorkerPool(WriteTracker writeTracker) {
        this(writeTracker, REPAIR_DELAY_MS);
    }

    /** Package-private constructor for tests that need a shorter dispatch delay. */
    RepairWorkerPool(WriteTracker writeTracker, long repairDelayMs) {
        this.writeTracker = writeTracker;
        this.repairDelayMs = repairDelayMs;
        this.running = false;
    }

    /**
     * Enqueues a repair operation with debounced, last-writer-wins compaction.
     *
     * <p>If a pending repair already exists for the same key with an equal or higher
     * seqOffset, the incoming operation is discarded. If the incoming seqOffset is
     * strictly higher, the pending operation is replaced and the scheduled execution
     * is reset.
     */
    public void enqueue(RepairOperation operation) {
        if (operation == null) {
            return;
        }
        String key = operation.blobKey();

        RepairOperation winner = pendingOperations.merge(key, operation, (existing, incoming) ->
                incoming.seqOffset() > existing.seqOffset() ? incoming : existing);

        if (winner != operation) {
            // Existing operation has equal or higher seqOffset — discard incoming
            logger.debug("Discarding stale repair: key={}, incoming={}, existing={}",
                    key, operation.seqOffset(), winner.seqOffset());
            return;
        }

        // Incoming won — cancel the previously scheduled future (if any) and reschedule
        ScheduledFuture<?> old = scheduledFutures.put(key,
                scheduler.schedule(() -> dispatchRepair(key), repairDelayMs, TimeUnit.MILLISECONDS));
        if (old != null) {
            old.cancel(false);
        }
        logger.debug("Enqueued repair: key={}, reason={}, seqOffset={}", key, operation.reason(), operation.seqOffset());
    }

    /**
     * Starts the worker pool. Must be called before any repair operations execute.
     */
    public void start() {
        if (running) {
            throw new IllegalStateException("RepairWorkerPool is already running");
        }
        running = true;
        logger.info("RepairWorkerPool started (virtual-thread workers)");
    }

    /**
     * Gracefully shuts down the worker pool and scheduler. Waits up to 10 seconds
     * for in-flight operations to complete.
     */
    public void shutdown() {
        running = false;
        scheduler.shutdownNow();
        workerPool.shutdownNow();
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("RepairWorkerPool did not terminate within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for RepairWorkerPool shutdown");
        }
        logger.info("RepairWorkerPool shut down. Remaining pending: {}", pendingOperations.size());
    }

    /**
     * Returns the current number of operations waiting to be dispatched.
     */
    public int pendingCount() {
        return pendingOperations.size();
    }

    /**
     * Returns whether the worker pool is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    private void dispatchRepair(String key) {
        scheduledFutures.remove(key);
        RepairOperation op = pendingOperations.remove(key);
        if (op == null) {
            return;
        }
        if (writeTracker.isActive(key)) {
            // Write in progress — re-enqueue so it retries after the blob lands
            logger.debug("Deferring repair for key={}: write in progress", key);
            enqueue(op);
            return;
        }
        workerPool.submit(() -> executeRepair(op));
    }

    /**
     * Executes a single repair operation.
     *
     * <p>This is a stub — the actual implementation would fetch the missing blob
     * from peer storage nodes or reconstruct it via erasure coding.
     */
    void executeRepair(RepairOperation operation) {
        // TODO: Implement actual repair logic:
        //  1. Identify peer nodes that hold a copy of this blob (via metadata/erasure set config)
        //  2. Fetch the blob data from a healthy peer via HTTP GET
        //  3. Write the blob to local disk using StorageNodeV2.store()
        //  4. Update the local database to mark the version as materialized
        //
        // For erasure-coded data:
        //  1. Fetch enough shards from peer nodes to reconstruct
        //  2. Run erasure decoding
        //  3. Write the reconstructed blob to local disk
        logger.info("Executing repair (stub): key={}, reason={}, seqOffset={}",
                operation.blobKey(), operation.reason(), operation.seqOffset());
    }
}
