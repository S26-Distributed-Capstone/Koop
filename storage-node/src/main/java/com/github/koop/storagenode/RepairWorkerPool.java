package com.github.koop.storagenode;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages a pool of virtual-thread workers that process {@link RepairOperation}s
 * from a persistent {@link RocksDbRepairQueue}.
 *
 * <p>A polling loop periodically scans the queue for pending operations.
 * Operations whose blobKey has an active write (per {@link WriteTracker}) are
 * deferred to the next poll cycle. Successfully completed operations are
 * deleted from the queue; failed operations remain for automatic retry.
 *
 * <p>On node startup, no distinct recovery phase is needed — the poller
 * naturally resumes processing any operations left in the queue from a
 * previous run.
 */
public class RepairWorkerPool {

    private static final Logger logger = LogManager.getLogger(RepairWorkerPool.class);

    static final long REPAIR_DELAY_MS = 2_000;

    private final RocksDbRepairQueue queue;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("repair-poller").factory());
    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
    private final WriteTracker writeTracker;
    private final BlobRepairStrategy repairStrategy;
    private final long repairDelayMs;
    private volatile boolean running;

    public RepairWorkerPool(RocksDbRepairQueue queue, WriteTracker writeTracker,
                            BlobRepairStrategy repairStrategy) {
        this(queue, writeTracker, REPAIR_DELAY_MS, repairStrategy);
    }

    RepairWorkerPool(RocksDbRepairQueue queue, WriteTracker writeTracker,
                     long repairDelayMs, BlobRepairStrategy repairStrategy) {
        this.queue = queue;
        this.writeTracker = writeTracker;
        this.repairDelayMs = repairDelayMs;
        this.repairStrategy = repairStrategy;
        this.running = false;
    }

    public void start() {
        if (running) {
            throw new IllegalStateException("RepairWorkerPool is already running");
        }
        running = true;
        long period = Math.max(repairDelayMs, 1);
        poller.scheduleWithFixedDelay(this::pollAndDispatch, period, period, TimeUnit.MILLISECONDS);
        logger.info("RepairWorkerPool started (persistent queue, virtual-thread workers)");
    }

    public void shutdown() {
        running = false;
        poller.shutdownNow();
        workerPool.shutdownNow();
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("RepairWorkerPool did not terminate within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for RepairWorkerPool shutdown");
        }
        logger.info("RepairWorkerPool shut down. Remaining pending: {}", queue.size());
    }

    public int pendingCount() {
        return queue.size();
    }

    public boolean isRunning() {
        return running;
    }

    private void pollAndDispatch() {
        if (!running) {
            return;
        }
        try {
            List<RocksDbRepairQueue.RepairEntry> entries = queue.pollAll(inFlight);
            for (var entry : entries) {
                RepairOperation op = entry.operation();

                if (writeTracker.isActive(op.blobKey())) {
                    logger.debug("Deferring repair for key={}: write in progress", op.blobKey());
                    continue;
                }

                if (!inFlight.add(op.blobKey())) {
                    continue;
                }

                final var e = entry;
                workerPool.submit(() -> executeRepair(e));
            }
        } catch (Exception e) {
            logger.error("Error during repair poll cycle", e);
        }
    }

    private void executeRepair(RocksDbRepairQueue.RepairEntry entry) {
        RepairOperation op = entry.operation();
        try {
            if (repairStrategy != null) {
                repairStrategy.repair(op);
            } else {
                logger.warn("No repair strategy configured; skipping repair for key={}",
                        op.blobKey());
            }
            queue.remove(entry.rocksKey(), op.blobKey());
        } catch (Exception e) {
            logger.error("Repair failed for key={}, will retry", op.blobKey(), e);
        } finally {
            inFlight.remove(op.blobKey());
        }
    }
}
