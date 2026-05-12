package com.github.koop.storagenode.gc;

import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks in-flight GET requests grouped by partition.
 *
 * <p>Each call to {@link #begin(int, long)} records that a client is currently
 * reading version {@code seqNum} of an object in {@code partition}. The returned
 * handle must be closed when the read completes so the entry is removed.
 *
 * <p>{@link #minActiveSeq(int)} returns the lowest sequence number currently
 * being read in that partition, used by the gossip-based GC watermark.
 */
public class ActiveReadTracker {

    private final ConcurrentMap<Integer, PartitionState> byPartition = new ConcurrentHashMap<>();

    private static final class PartitionState {
        // seqNum -> reference count
        final TreeMap<Long, Integer> counts = new TreeMap<>();
    }

    public Handle begin(int partition, long seqNum) {
        PartitionState state = byPartition.computeIfAbsent(partition, k -> new PartitionState());
        synchronized (state) {
            state.counts.merge(seqNum, 1, Integer::sum);
        }
        return new Handle(partition, seqNum);
    }

    private void end(int partition, long seqNum) {
        PartitionState state = byPartition.get(partition);
        if (state == null) return;
        synchronized (state) {
            Integer count = state.counts.get(seqNum);
            if (count == null) return;
            if (count <= 1) {
                state.counts.remove(seqNum);
            } else {
                state.counts.put(seqNum, count - 1);
            }
        }
    }

    /**
     * Lowest sequence number with at least one active GET in this partition,
     * or empty if no GET is active.
     */
    public OptionalLong minActiveSeq(int partition) {
        PartitionState state = byPartition.get(partition);
        if (state == null) return OptionalLong.empty();
        synchronized (state) {
            if (state.counts.isEmpty()) return OptionalLong.empty();
            return OptionalLong.of(state.counts.firstKey());
        }
    }

    public final class Handle implements AutoCloseable {
        private final int partition;
        private final long seqNum;
        private boolean closed = false;

        Handle(int partition, long seqNum) {
            this.partition = partition;
            this.seqNum = seqNum;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            end(partition, seqNum);
        }
    }
}
