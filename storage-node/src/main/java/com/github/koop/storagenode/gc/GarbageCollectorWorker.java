package com.github.koop.storagenode.gc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * <p>For each partition, the worker:
 * <ol>
 *   <li>Looks up the partition's global watermark; if absent it skips that
 *       partition (no consensus yet).</li>
 *   <li>Walks the partition's oplog forward, stopping at the first entry whose
 *       {@code seqNum >= watermark}.</li>
 *   <li>For each candidate, asks the database to GC that specific version. The
 *       database enforces the "keep active latest live version" invariant and
 *       atomically updates metadata + oplog.</li>
 *   <li>Deletes the on-disk blob file if a regular version was removed.</li>
 * </ol>
 *
 * <p>The single shared RocksDB instance means partitions are processed
 * sequentially within a tick; a single-threaded scheduler is fine.
 */
public class GarbageCollectorWorker {

    private static final Logger logger = LogManager.getLogger(GarbageCollectorWorker.class);

    private final Database db;
    private final PartitionWatermarks watermarks;
    private final IntFunction<Set<Integer>> ownedPartitions;
    private final Path storageDir;
    private final long staleAfterMs;
    private final long gcIntervalMs;
    private final ScheduledExecutorService scheduler;

    public GarbageCollectorWorker(Database db,
                                  PartitionWatermarks watermarks,
                                  IntFunction<Set<Integer>> ownedPartitions,
                                  Path storageDir,
                                  long staleAfterMs,
                                  long gcIntervalMs) {
        this.db = db;
        this.watermarks = watermarks;
        this.ownedPartitions = ownedPartitions;
        this.storageDir = storageDir;
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

    /** One pass over all owned partitions. Returns total records removed. */
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
        int removed = 0;
        // Snapshot candidates first so we don't iterate-and-mutate the same column family.
        List<OpLog> candidates;
        try (var stream = db.streamOpLog(partition)) {
            candidates = stream
                    .takeWhile(op -> op.seqNum() < watermark)
                    .filter(op -> op.operation() == Operation.PUT || op.operation() == Operation.DELETE)
                    .toList();
        } catch (Exception e) {
            logger.warn("GC: failed to scan oplog for partition {}: {}", partition, e.getMessage());
            return 0;
        }

        for (OpLog op : candidates) {
            try {
                Optional<GcResult> result = db.gcCleanupVersion(op.key(), partition, op.seqNum());
                if (result.isPresent()) {
                    removed++;
                    result.get().blobLocation().ifPresent(this::deleteBlobFile);
                }
            } catch (Exception e) {
                logger.warn("GC: failed to clean key={} partition={} seq={}: {}",
                        op.key(), partition, op.seqNum(), e.getMessage());
            }
        }
        if (removed > 0) {
            logger.debug("GC partition={} watermark={} removed={}", partition, watermark, removed);
        }
        return removed;
    }

    private void deleteBlobFile(String location) {
        if (location == null || location.isBlank()) return;
        if (location.contains("..") || location.contains("/") || location.contains("\\")) {
            logger.warn("GC: refusing to delete blob with suspicious location: {}", location);
            return;
        }
        String prefixDir = location.length() >= 3 ? location.substring(0, 3) : "000";
        Path path = storageDir.normalize().resolve("blobs").resolve(prefixDir).resolve(location).normalize();
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            logger.warn("GC: failed to delete blob file {}: {}", path, e.getMessage());
        }
    }
}
