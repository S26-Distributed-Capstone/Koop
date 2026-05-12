package com.github.koop.storagenode.gc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-partition aggregation of gossiped local minimums from peer storage nodes.
 *
 * <p>Each peer is identified by {@code nodeId} (typically {@code ip:port}); we
 * retain the last-reported (seqNum, timestamp) pair. The global watermark for
 * a partition is the minimum over all non-stale entries — peers that have not
 * gossiped within {@code staleAfterMs} are excluded so a single down node
 * cannot stall progress indefinitely.
 *
 * <p>If no entry exists for a partition at all, {@link #watermarkFor(int, long)}
 * returns empty — GC must skip that partition rather than wipe everything.
 */
public class PartitionWatermarks {

    private final ConcurrentMap<Integer, PartitionState> byPartition = new ConcurrentHashMap<>();

    private record Entry(long minSeq, long timestampMs) {}

    private static final class PartitionState {
        final Map<String, Entry> byNode = new HashMap<>();
    }

    /** Record a peer's latest gossiped state for one partition. */
    public void update(String nodeId, int partition, long minSeq, long timestampMs) {
        PartitionState state = byPartition.computeIfAbsent(partition, k -> new PartitionState());
        synchronized (state) {
            state.byNode.put(nodeId, new Entry(minSeq, timestampMs));
        }
    }

    /**
     * Compute the current global watermark for a partition, evicting entries
     * older than {@code staleAfterMs}. Returns empty if no fresh entries exist.
     */
    public OptionalLong watermarkFor(int partition, long staleAfterMs) {
        PartitionState state = byPartition.get(partition);
        if (state == null) return OptionalLong.empty();
        long cutoff = System.currentTimeMillis() - staleAfterMs;
        long min = Long.MAX_VALUE;
        boolean any = false;
        synchronized (state) {
            Iterator<Map.Entry<String, Entry>> it = state.byNode.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Entry> e = it.next();
                Entry entry = e.getValue();
                if (entry.timestampMs() < cutoff) {
                    it.remove();
                    continue;
                }
                if (entry.minSeq() < min) min = entry.minSeq();
                any = true;
            }
        }
        return any ? OptionalLong.of(min) : OptionalLong.empty();
    }

    /** Drop all state for a partition (e.g. when this node is unassigned). */
    public void forgetPartition(int partition) {
        byPartition.remove(partition);
    }
}
