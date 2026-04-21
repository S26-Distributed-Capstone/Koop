package com.github.koop.storagenode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages a bounded concurrent worker pool that processes {@link RepairOperation}s
 * from an in-memory {@link BlockingQueue}.
 *
 * <p>Each worker takes repair operations from the queue and executes the
 * (currently stubbed) repair logic. The pool processes exactly {@code N}
 * operations concurrently using a fixed-size thread pool.
 */
public class RepairWorkerPool {

    private static final Logger logger = LogManager.getLogger(RepairWorkerPool.class);

    /** Default number of concurrent repair workers. */
    public static final int DEFAULT_CONCURRENCY = 4;

    private final BlockingQueue<RepairOperation> queue;
    private final ExecutorService workerPool;
    private final int concurrency;
    private volatile boolean running;

    public RepairWorkerPool() {
        this(DEFAULT_CONCURRENCY);
    }

    public RepairWorkerPool(int concurrency) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be positive, got: " + concurrency);
        }
        this.concurrency = concurrency;
        this.queue = new LinkedBlockingQueue<>();
        this.workerPool = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r);
            t.setName("repair-worker-" + t.threadId());
            t.setDaemon(true);
            return t;
        });
        this.running = false;
    }

    /**
     * Thread-safe enqueue of a repair operation. Can be called concurrently
     * from read-repair triggers and startup catch-up processing.
     *
     * @param operation the repair operation to enqueue
     */
    public void enqueue(RepairOperation operation) {
        if (operation == null) {
            return;
        }
        boolean offered = queue.offer(operation);
        if (offered) {
            logger.debug("Enqueued repair operation: key={}, reason={}", operation.blobKey(), operation.reason());
        } else {
            logger.warn("Failed to enqueue repair operation (queue full): key={}", operation.blobKey());
        }
    }

    /**
     * Starts the worker pool. Each worker loops, taking operations from the
     * queue and executing the repair logic.
     */
    public void start() {
        if (running) {
            throw new IllegalStateException("RepairWorkerPool is already running");
        }
        running = true;
        logger.info("Starting RepairWorkerPool with {} workers", concurrency);

        for (int i = 0; i < concurrency; i++) {
            workerPool.submit(this::workerLoop);
        }
    }

    /**
     * Gracefully shuts down the worker pool. Interrupts workers waiting on the
     * queue and waits up to 10 seconds for in-flight operations to complete.
     */
    public void shutdown() {
        running = false;
        workerPool.shutdownNow();
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("RepairWorkerPool did not terminate within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for RepairWorkerPool shutdown");
        }
        logger.info("RepairWorkerPool shut down. Remaining queue size: {}", queue.size());
    }

    /**
     * Returns the current number of pending repair operations in the queue.
     */
    public int pendingCount() {
        return queue.size();
    }

    /**
     * Returns whether the worker pool is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    private void workerLoop() {
        logger.debug("Repair worker started: {}", Thread.currentThread().getName());
        while (running) {
            try {
                RepairOperation operation = queue.take(); // blocks until available
                executeRepair(operation);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in repair worker", e);
            }
        }
        logger.debug("Repair worker stopped: {}", Thread.currentThread().getName());
    }

    /**
     * Executes a single repair operation.
     *
     * <p>This is a stub — the actual implementation would fetch the missing blob
     * from peer storage nodes or reconstruct it via erasure coding.
     *
     * @param operation the repair operation to execute
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
