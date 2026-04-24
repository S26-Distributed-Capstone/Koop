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
 *
 * <p>Actual repair work is delegated to a {@link BlobRepairStrategy} supplied
 * at construction time, keeping this class free of direct dependencies on
 * {@link StorageNodeV2} or HTTP clients.
 */
public class RepairWorkerPool implements RepairQueue {

    private static final Logger logger = LogManager.getLogger(RepairWorkerPool.class);

    static final long REPAIR_DELAY_MS = 2_000;

    private final ConcurrentHashMap<String, RepairOperation> pendingOperations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> scheduledFutures = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("repair-scheduler").factory());
    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
    private final WriteTracker writeTracker;
    private final BlobRepairStrategy repairStrategy;
    private final long repairDelayMs;
    private volatile boolean running;

    public RepairWorkerPool(WriteTracker writeTracker, BlobRepairStrategy repairStrategy) {
        this(writeTracker, REPAIR_DELAY_MS, repairStrategy);
    }

    /** Package-private constructor for tests that need a shorter dispatch delay. */
    RepairWorkerPool(WriteTracker writeTracker, long repairDelayMs, BlobRepairStrategy repairStrategy) {
        this.writeTracker = writeTracker;
        this.repairDelayMs = repairDelayMs;
        this.repairStrategy = repairStrategy;
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
    @Override
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
        logger.debug("Enqueued repair: key={}, seqOffset={}", key, operation.seqOffset());
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
     * Executes a single repair operation by delegating to the configured
     * {@link BlobRepairStrategy}.
     */
    void executeRepair(RepairOperation operation) {
        if (repairStrategy != null) {
            repairStrategy.repair(operation);
        } else {
            logger.warn("No repair strategy configured; skipping repair for key={}", operation.blobKey());
        }
    }
}
