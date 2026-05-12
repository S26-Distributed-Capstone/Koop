package com.github.koop.storagenode.gc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;

/**
 * Disk reconciliation worker that drains the durable {@code pending_deletes}
 * queue maintained by {@link Database#gcCleanupVersion(String, int, long)}.
 *
 * <p>Decoupling physical blob deletion from metadata cleanup makes the system
 * crash-consistent: if the node dies between the metadata commit and the
 * filesystem unlink, the pending-deletion entry survives in RocksDB and is
 * re-processed on startup, so no untracked blobs accumulate on disk.
 *
 * <p>Algorithm per tick:
 * <ol>
 *   <li>Snapshot the queue (RocksDB iterator → list).</li>
 *   <li>For each location, attempt {@link Files#deleteIfExists} at the
 *       sharded blob path.</li>
 *   <li>On success (or if the file was already absent), remove the entry from
 *       the queue. On I/O failure leave the entry so the next pass retries.</li>
 * </ol>
 */
public class BlobDeletionWorker {

    private static final Logger logger = LogManager.getLogger(BlobDeletionWorker.class);

    private final Database db;
    private final Path storageDir;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler;

    public BlobDeletionWorker(Database db, Path storageDir, long intervalMs) {
        this.db = db;
        this.storageDir = storageDir;
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("blob-deletion-").factory());
    }

    public synchronized void start() {
        scheduler.scheduleAtFixedRate(this::runOnceQuiet,
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Drain the pending-deletions queue once. Returns the number of blobs reclaimed. */
    public int runOnce() {
        List<String> snapshot;
        try (var stream = db.pendingBlobDeletions()) {
            snapshot = stream.toList();
        } catch (Exception e) {
            logger.warn("Failed to scan pending-deletions queue: {}", e.getMessage(), e);
            return 0;
        }

        int reclaimed = 0;
        for (String location : snapshot) {
            if (tryDelete(location)) {
                try {
                    db.removePendingBlobDeletion(location);
                    reclaimed++;
                } catch (Exception e) {
                    logger.warn("Failed to dequeue pending deletion for {}: {}",
                            location, e.getMessage(), e);
                }
            }
        }
        return reclaimed;
    }

    private void runOnceQuiet() {
        try {
            int reclaimed = runOnce();
            if (reclaimed > 0) {
                logger.info("Blob deletion sweep reclaimed {} file(s)", reclaimed);
            }
        } catch (Exception e) {
            logger.warn("Blob deletion sweep failed: {}", e.getMessage(), e);
        }
    }

    private boolean tryDelete(String location) {
        if (location == null || location.isBlank()) return true;
        if (location.contains("..") || location.contains("/") || location.contains("\\")) {
            logger.warn("Refusing to delete blob with suspicious location: {}", location);
            return true; // drop from queue — we'll never delete a malformed entry
        }
        String prefixDir = location.length() >= 3 ? location.substring(0, 3) : "000";
        Path path = storageDir.normalize().resolve("blobs").resolve(prefixDir).resolve(location).normalize();
        try {
            Files.deleteIfExists(path);
            return true;
        } catch (IOException e) {
            logger.warn("Failed to delete blob file {}: {} — will retry", path, e.getMessage());
            return false;
        }
    }
}
