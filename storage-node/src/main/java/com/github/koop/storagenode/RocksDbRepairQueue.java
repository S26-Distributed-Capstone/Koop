package com.github.koop.storagenode;

import com.github.koop.storagenode.db.RocksDbStorageStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RocksDbRepairQueue implements RepairQueue {

    private static final Logger logger = LogManager.getLogger(RocksDbRepairQueue.class);

    private final RocksDbStorageStrategy strategy;
    private final AtomicLong sequence;
    private final ConcurrentHashMap<String, PendingEntry> blobKeyIndex;

    private record PendingEntry(byte[] rocksKey, long seqOffset) {}

    public RocksDbRepairQueue(RocksDbStorageStrategy strategy) {
        this.strategy = strategy;
        this.blobKeyIndex = new ConcurrentHashMap<>();
        this.sequence = new AtomicLong(0);
        rebuildIndex();
    }

    private void rebuildIndex() {
        try (RocksIterator it = strategy.newRepairQueueIterator()) {
            it.seekToFirst();
            long maxSeq = 0;
            while (it.isValid()) {
                byte[] key = it.key();
                long seq = ByteBuffer.wrap(key).getLong();
                if (seq > maxSeq) {
                    maxSeq = seq;
                }

                RepairOperation op;
                try {
                    op = RepairOperation.deserialize(it.value());
                } catch (Exception e) {
                    logger.warn("Skipping corrupt repair queue entry at seq={}", seq, e);
                    try {
                        strategy.deleteRepairEntry(key);
                    } catch (Exception de) {
                        logger.warn("Failed to delete corrupt entry", de);
                    }
                    it.next();
                    continue;
                }

                PendingEntry existing = blobKeyIndex.get(op.blobKey());
                if (existing == null || op.seqOffset() > existing.seqOffset()) {
                    if (existing != null) {
                        try {
                            strategy.deleteRepairEntry(existing.rocksKey());
                        } catch (Exception e) {
                            logger.warn("Failed to clean up duplicate repair entry during rebuild", e);
                        }
                    }
                    blobKeyIndex.put(op.blobKey(), new PendingEntry(key.clone(), op.seqOffset()));
                } else {
                    try {
                        strategy.deleteRepairEntry(key);
                    } catch (Exception e) {
                        logger.warn("Failed to clean up stale repair entry during rebuild", e);
                    }
                }

                it.next();
            }
            sequence.set(maxSeq);
            logger.info("RepairQueue rebuilt: {} pending operations, next sequence={}",
                    blobKeyIndex.size(), maxSeq + 1);
        }
    }

    @Override
    public void enqueue(RepairOperation operation) {
        if (operation == null) {
            return;
        }

        String blobKey = operation.blobKey();
        PendingEntry existing = blobKeyIndex.get(blobKey);
        if (existing != null && existing.seqOffset() >= operation.seqOffset()) {
            logger.debug("Discarding stale repair: key={}, incoming={}, existing={}",
                    blobKey, operation.seqOffset(), existing.seqOffset());
            return;
        }

        byte[] rocksKey = longToBytes(sequence.incrementAndGet());
        byte[] value = operation.serialize();

        try {
            strategy.putRepairEntry(rocksKey, value);
            PendingEntry old = blobKeyIndex.put(blobKey,
                    new PendingEntry(rocksKey, operation.seqOffset()));
            if (old != null) {
                try {
                    strategy.deleteRepairEntry(old.rocksKey());
                } catch (Exception e) {
                    logger.warn("Failed to delete superseded repair entry for key={}", blobKey, e);
                }
            }
            logger.debug("Enqueued repair: key={}, seqOffset={}", blobKey, operation.seqOffset());
        } catch (Exception e) {
            logger.error("Failed to persist repair operation for key={}", blobKey, e);
        }
    }

    /**
     * Returns all pending repair entries whose blobKey is not in the exclude set,
     * in chronological order (oldest first).
     */
    public List<RepairEntry> pollAll(Set<String> excludeBlobKeys) {
        List<RepairEntry> entries = new ArrayList<>();
        try (RocksIterator it = strategy.newRepairQueueIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                RepairOperation op;
                try {
                    op = RepairOperation.deserialize(it.value());
                } catch (Exception e) {
                    it.next();
                    continue;
                }
                if (!excludeBlobKeys.contains(op.blobKey())) {
                    entries.add(new RepairEntry(it.key().clone(), op));
                }
                it.next();
            }
        }
        return entries;
    }

    public void remove(byte[] rocksKey, String blobKey) {
        try {
            strategy.deleteRepairEntry(rocksKey);
            blobKeyIndex.computeIfPresent(blobKey, (k, existing) -> {
                if (Arrays.equals(existing.rocksKey(), rocksKey)) {
                    return null;
                }
                return existing;
            });
            logger.debug("Removed completed repair: key={}", blobKey);
        } catch (Exception e) {
            logger.error("Failed to remove repair entry for key={}", blobKey, e);
        }
    }

    public int size() {
        return blobKeyIndex.size();
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    public record RepairEntry(byte[] rocksKey, RepairOperation operation) {}
}
