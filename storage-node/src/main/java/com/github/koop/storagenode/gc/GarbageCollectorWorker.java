package com.github.koop.storagenode.gc;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.Database.GcResult;
import com.github.koop.storagenode.db.OpLog;
import com.github.koop.storagenode.db.Operation;

/**
 * Background worker that periodically scans every partition this node owns and
 * physically removes obsolete records from RocksDB based on the current gossip-
 * computed watermark.
 *
 * <p>Each partition keeps a persistent {@code gc_cursor} (a seqNum) so each
 * pass only examines oplog entries that have arrived since the previous tick.
 * This bounds work per cycle to O(new operations) instead of
 * O(total historical operations) — the cursor lives in its own RocksDB column
 * family and survives restarts.
 *
 * <p>For each partition the worker:
 * <ol>
 *   <li>Looks up the partition's gossip-derived watermark; skips if absent.</li>
 *   <li>Reads the persisted GC cursor (defaults to 0).</li>
 *   <li>Streams the oplog forward from the cursor, takeWhile {@code seqNum < watermark}.</li>
 *   <li>For each oplog entry, asks {@link Database#gcCleanupKey} to reap stale
 *       versions for that key in a transaction.</li>
 *   <li>Advances the persisted cursor to the watermark so the next pass starts
 *       there.</li>
 * </ol>
 *
 * <p>Physical blob deletion is decoupled: {@code gcCleanupKey} enqueues an
 * entry into a durable {@code pending_deletes} column family, and
 * {@link BlobDeletionWorker} sweeps that queue separately.
 *
 * <p>The single shared RocksDB instance means partitions are processed
 * sequentially within a tick; a single-threaded scheduler is fine.
 */
public class GarbageCollectorWorker {

    private static final Logger logger = LogManager.getLogger(GarbageCollectorWorker.class);

    private final Database db;
    private final PartitionWatermarks watermarks;
    private final IntFunction<Set<Integer>> ownedPartitions;
    private final long staleAfterMs;
    private final long gcIntervalMs;
    private final ScheduledExecutorService scheduler;

    public GarbageCollectorWorker(Database db,
                                  PartitionWatermarks watermarks,
                                  IntFunction<Set<Integer>> ownedPartitions,
                                  long staleAfterMs,
                                  long gcIntervalMs) {
        this.db = db;
        this.watermarks = watermarks;
        this.ownedPartitions = ownedPartitions;
        this.staleAfterMs = staleAfterMs;
        this.gcIntervalMs = gcIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("gc-worker-").factory());
    }

    public synchronized void start() {
        scheduler.scheduleAtFixedRate(this::runOnceQuiet,
                gcIntervalMs, gcIntervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** One pass over all owned partitions. Returns total versions removed. */
    public int runOnce() {
        int total = 0;
        for (Integer partition : ownedPartitions.apply(0)) {
            OptionalLong wm = watermarks.watermarkFor(partition, staleAfterMs);
            if (wm.isEmpty()) {
                logger.debug("Skipping partition {} — no gossip watermark yet", partition);
                continue;
            }
            total += runPartition(partition, wm.getAsLong());
        }
        return total;
    }

    private void runOnceQuiet() {
        try {
            int removed = runOnce();
            if (removed > 0) {
                logger.info("GC pass removed {} record(s)", removed);
            }
        } catch (Exception e) {
            logger.warn("GC pass failed: {}", e.getMessage(), e);
        }
    }

    private int runPartition(int partition, long watermark) {
        long cursor;
        try {
            cursor = db.getGcCursor(partition);
        } catch (Exception e) {
            logger.warn("GC: failed to read cursor for partition {}: {}", partition, e.getMessage());
            return 0;
        }

        if (cursor >= watermark) {
            // Nothing new to examine since the last cycle.
            return 0;
        }

        List<OpLog> candidates;
        try (var stream = db.streamOpLog(partition, cursor)) {
            candidates = stream
                    .takeWhile(op -> op.seqNum() < watermark)
                    .filter(op -> op.operation() == Operation.PUT || op.operation() == Operation.DELETE)
                    .toList();
        } catch (Exception e) {
            logger.warn("GC: failed to scan oplog for partition {} from cursor {}: {}",
                    partition, cursor, e.getMessage());
            return 0;
        }

        int removed = 0;
        for (OpLog op : candidates) {
            try {
                Optional<GcResult> result = db.gcCleanupKey(op.key(), partition, op.seqNum(), watermark);
                if (result.isPresent()) {
                    removed += result.get().versionsRemoved();
                }
            } catch (Exception e) {
                logger.warn("GC: failed to clean key={} partition={} seq={}: {}",
                        op.key(), partition, op.seqNum(), e.getMessage());
            }
        }

        // Advance the cursor to the watermark — every oplog entry in
        // [cursor, watermark) has been examined. If we crash mid-pass, the
        // next cycle re-scans from the old cursor; gcCleanupKey is idempotent.
        try {
            db.setGcCursor(partition, watermark);
        } catch (Exception e) {
            logger.warn("GC: failed to persist cursor for partition {}: {}", partition, e.getMessage());
        }

        if (removed > 0) {
            logger.debug("GC partition={} cursor {}→{} removed={}",
                    partition, cursor, watermark, removed);
        }
        return removed;
    }
}
