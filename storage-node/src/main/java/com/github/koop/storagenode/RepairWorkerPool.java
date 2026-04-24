package com.github.koop.storagenode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages a concurrent worker pool that processes {@link RepairOperation}s with
 * debounced, last-writer-wins compaction.
 *
 * <p>When the same key is enqueued multiple times before execution, only the
 * most recent operation is retained. A short delay between enqueue and execution
 * lets rapid sequential updates compact and gives in-progress writes time to
 * finish before a repair is attempted.
 */
public class RepairWorkerPool {

    private static final Logger logger = LogManager.getLogger(RepairWorkerPool.class);

    public static final int DEFAULT_CONCURRENCY = 4;
    static final long REPAIR_DELAY_MS = 2_000;

    private final ConcurrentHashMap<String, RepairOperation> pendingOperations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> scheduledFutures = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "repair-scheduler");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService workerPool;
    private final Predicate<String> activeWriteCheck;
    private final long repairDelayMs;
    private final int concurrency;
    private volatile boolean running;

    public RepairWorkerPool() {
        this(DEFAULT_CONCURRENCY, key -> false, REPAIR_DELAY_MS);
    }

    public RepairWorkerPool(int concurrency) {
        this(concurrency, key -> false, REPAIR_DELAY_MS);
    }

    public RepairWorkerPool(int concurrency, Predicate<String> activeWriteCheck) {
        this(concurrency, activeWriteCheck, REPAIR_DELAY_MS);
    }

    /** Package-private constructor for tests that need a shorter dispatch delay. */
    RepairWorkerPool(int concurrency, Predicate<String> activeWriteCheck, long repairDelayMs) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be positive, got: " + concurrency);
        }
        this.concurrency = concurrency;
        this.activeWriteCheck = activeWriteCheck;
        this.repairDelayMs = repairDelayMs;
        this.workerPool = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r);
            t.setName("repair-worker-" + t.threadId());
            t.setDaemon(true);
            return t;
        });
        this.running = false;
    }

    /**
     * Enqueues a repair operation with debounced, last-writer-wins compaction.
     * If a pending repair exists for the same key, it is overwritten and its
     * scheduled execution is replaced.
     */
    public void enqueue(RepairOperation operation) {
        if (operation == null) {
            return;
        }
        String key = operation.blobKey();
        pendingOperations.put(key, operation);
        ScheduledFuture<?> old = scheduledFutures.put(key,
                scheduler.schedule(() -> dispatchRepair(key), repairDelayMs, TimeUnit.MILLISECONDS));
        if (old != null) {
            old.cancel(false);
        }
        logger.debug("Enqueued repair operation: key={}, reason={}", key, operation.reason());
    }

    /**
     * Starts the worker pool. Must be called before any repair operations execute.
     */
    public void start() {
        if (running) {
            throw new IllegalStateException("RepairWorkerPool is already running");
        }
        running = true;
        logger.info("Starting RepairWorkerPool with {} workers", concurrency);
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
        if (activeWriteCheck.test(key)) {
            // A write is in progress — re-enqueue so it retries after the blob lands
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
        logger.info("Executing repair operation (stub): key={}, reason={}", operation.blobKey(), operation.reason());
    }
}
