package com.github.koop.storagenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.FileVersion;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.MultipartFileVersion;
import com.github.koop.storagenode.db.RegularFileVersion;
import com.github.koop.storagenode.db.TombstoneFileVersion;

/**
 * Background worker that physically deletes shard files (and multipart chunks)
 * whose versions have fallen below the gossip-derived safe-deletion watermark,
 * and compacts the corresponding entries out of the metadata table.
 *
 * <p>Modeled after {@link RepairWorkerPool}: a single-threaded scheduled
 * poller wakes periodically, walks every metadata row, and consults the
 * {@link GossipService} for the per-partition watermark. For each row it
 * removes versions whose sequence number is strictly less than the partition
 * watermark via {@link Database#compactMetadata(String, long)} and then
 * unlinks the on-disk files referenced by the removed versions.
 */
public class GarbageCollectionWorker {

    private static final Logger logger = LogManager.getLogger(GarbageCollectionWorker.class);

    static final long DEFAULT_GC_INTERVAL_MS = 5_000;

    private final Database db;
    private final StorageNodeV2 storageNode;
    private final GossipService gossipService;
    private final WriteTracker writeTracker;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("gc-worker").factory());
    private volatile boolean running = false;

    public GarbageCollectionWorker(Database db, StorageNodeV2 storageNode,
                                   GossipService gossipService, WriteTracker writeTracker) {
        this(db, storageNode, gossipService, writeTracker, DEFAULT_GC_INTERVAL_MS);
    }

    GarbageCollectionWorker(Database db, StorageNodeV2 storageNode,
                            GossipService gossipService, WriteTracker writeTracker,
                            long intervalMs) {
        this.db = db;
        this.storageNode = storageNode;
        this.gossipService = gossipService;
        this.writeTracker = writeTracker;
        this.intervalMs = intervalMs;
    }

    public void start() {
        if (running) {
            throw new IllegalStateException("GarbageCollectionWorker already running");
        }
        running = true;
        long period = Math.max(intervalMs, 1);
        scheduler.scheduleWithFixedDelay(this::runOnce, period, period, TimeUnit.MILLISECONDS);
        logger.info("GarbageCollectionWorker started, interval={}ms", intervalMs);
    }

    public void shutdown() {
        running = false;
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("GarbageCollectionWorker did not shut down within 10s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Runs a single GC pass: scan every metadata row, drop versions that fall
     * strictly below the gossip safe watermark for their partition, then unlink
     * the physical blob/chunk files that those versions referenced.
     *
     * <p>Returns the number of physical files deleted in this pass. Exposed for
     * tests and for explicit on-demand invocation.
     */
    public int runOnce() {
        int filesDeleted = 0;
        try {
            List<Metadata> rows = new ArrayList<>();
            try (Stream<Metadata> stream = db.streamAllMetadata()) {
                stream.forEach(rows::add);
            }
            for (Metadata metadata : rows) {
                filesDeleted += compactSingleKey(metadata);
            }
        } catch (Exception e) {
            logger.error("GC pass failed", e);
        }
        return filesDeleted;
    }

    private int compactSingleKey(Metadata metadata) {
        // Defer compaction while a write is in flight for this key — a
        // concurrent putItem could be about to materialize a version, and we
        // don't want to race the writer through the same metadata row.
        if (writeTracker.isActive(metadata.key())) {
            logger.debug("Skipping GC for key={}: write in progress", metadata.key());
            return 0;
        }

        OptionalLong watermarkOpt = gossipService.safeWatermark(metadata.partition());
        if (watermarkOpt.isEmpty()) {
            return 0;
        }
        long watermark = watermarkOpt.getAsLong();

        List<FileVersion> removed;
        try {
            removed = db.compactMetadata(metadata.key(), watermark);
        } catch (Exception e) {
            logger.error("Failed to compact metadata for key={}", metadata.key(), e);
            return 0;
        }

        int deleted = 0;
        for (FileVersion v : removed) {
            deleted += deletePhysicalFiles(v);
        }
        if (!removed.isEmpty()) {
            logger.debug("GC compacted key={}: removed {} versions (watermark={})",
                    metadata.key(), removed.size(), watermark);
        }
        return deleted;
    }

    private int deletePhysicalFiles(FileVersion version) {
        int deleted = 0;
        if (version instanceof RegularFileVersion r) {
            if (tryDelete(r.location())) {
                deleted++;
            }
        } else if (version instanceof MultipartFileVersion m) {
            for (String chunk : m.chunks()) {
                if (tryDelete(chunk)) {
                    deleted++;
                }
            }
        } else if (version instanceof TombstoneFileVersion) {
            // No physical state — the tombstone marker itself was removed by
            // metadata compaction.
        }
        return deleted;
    }

    private boolean tryDelete(String blobId) {
        try {
            storageNode.deleteBlobFile(blobId);
            return true;
        } catch (IOException e) {
            logger.warn("Failed to delete blob file id={}", blobId, e);
            return false;
        } catch (IllegalArgumentException e) {
            // Defensive: a malformed blobId stored long ago shouldn't crash GC
            logger.warn("Refusing to delete blob with invalid id={}", blobId, e);
            return false;
        }
    }
}
