package com.github.koop.storagenode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks per-partition state needed by the garbage-collection gossip protocol:
 * <ul>
 *   <li>The highest sequence number this node has processed for each partition
 *       (the node is "currently up to" this value).</li>
 *   <li>The sequence numbers of GET requests currently being served — these
 *       sequence numbers cannot be reaped until the GETs complete.</li>
 * </ul>
 *
 * <p>The lowest sequence number still in use on this node, for a given
 * partition, is defined as
 * {@code min(highWatermark[p], minActiveGetSeq[p])}. A partition for which
 * neither value is known is omitted from the snapshot; the gossip layer
 * treats absence as "no watermark, do not garbage collect anything here".
 *
 * <p>Active GETs are tracked with reference counts so that two concurrent
 * GETs serving the same sequence number cannot accidentally unpin the
 * version when the first one finishes.
 */
public class ActiveSequenceTracker {

    private final Map<Integer, Long> highWatermark = new ConcurrentHashMap<>();
    private final Map<Integer, ConcurrentSkipListMap<Long, AtomicInteger>> activeGets = new ConcurrentHashMap<>();

    /**
     * Record that this node has processed sequence number {@code seqNum}
     * for {@code partition}. Monotonically advances the high-water mark.
     */
    public void recordProcessedSeq(int partition, long seqNum) {
        highWatermark.merge(partition, seqNum, Math::max);
    }

    /** Mark a GET on {@code partition} that is serving sequence {@code seqNum}. */
    public void beginGet(int partition, long seqNum) {
        activeGets.computeIfAbsent(partition, p -> new ConcurrentSkipListMap<>())
                  .computeIfAbsent(seqNum, s -> new AtomicInteger())
                  .incrementAndGet();
    }

    /**
     * Release a GET previously registered with {@link #beginGet(int, long)}.
     * The entry is removed once the in-flight count for that sequence drops
     * to zero — concurrent GETs on the same sequence are tracked by count.
     */
    public void endGet(int partition, long seqNum) {
        ConcurrentSkipListMap<Long, AtomicInteger> map = activeGets.get(partition);
        if (map == null) {
            return;
        }
        map.computeIfPresent(seqNum, (k, count) -> count.decrementAndGet() <= 0 ? null : count);
    }

    /**
     * Returns this node's lowest sequence number still in use for each
     * partition it knows about. Partitions absent from the result are
     * partitions the node has no information about, and the gossip layer
     * deliberately omits them from the broadcast.
     */
    public Map<Integer, Long> snapshotLowestInUse() {
        Map<Integer, Long> result = new HashMap<>();
        Set<Integer> partitions = new java.util.HashSet<>(highWatermark.keySet());
        partitions.addAll(activeGets.keySet());
        for (Integer partition : partitions) {
            Long hw = highWatermark.get(partition);
            ConcurrentSkipListMap<Long, AtomicInteger> map = activeGets.get(partition);
            Long lowestGet = null;
            if (map != null) {
                var entry = map.firstEntry();
                if (entry != null) {
                    lowestGet = entry.getKey();
                }
            }
            long value;
            if (hw == null && lowestGet == null) {
                continue;
            } else if (hw == null) {
                value = lowestGet;
            } else if (lowestGet == null) {
                value = hw;
            } else {
                value = Math.min(hw, lowestGet);
            }
            result.put(partition, value);
        }
        return result;
    }

    /** Test helper: current high-water mark for {@code partition}, or {@code -1} if none. */
    long highWatermark(int partition) {
        return highWatermark.getOrDefault(partition, -1L);
    }
}
