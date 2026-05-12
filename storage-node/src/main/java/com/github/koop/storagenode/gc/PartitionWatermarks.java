package com.github.koop.storagenode.gc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/**
 * Per-partition aggregation of gossiped local minimums from peer storage nodes.
 *
 * <p>Each peer is identified by {@code nodeId} (typically {@code ip:port}); we
 * retain the last-reported (seqNum, lastSeenMs) pair. The global watermark for
 * a partition is the minimum over all non-stale entries — peers that have not
 * been heard from within {@code staleAfterMs} are excluded so a single down
 * node cannot stall progress indefinitely.
 *
 * <p><b>Staleness is measured strictly by the receiver's clock:</b> on every
 * {@link #update(String, int, long)}, we record
 * {@code clock.getAsLong()} — not the sender's wall-clock timestamp — as the
 * {@code lastSeenMs}. This neutralizes peer clock drift: if a peer's clock is
 * skewed by hours, it has no effect on when its entries time out, since the
 * receiver only ever asks "how long has it been since I heard from this peer?".
 *
 * <p>If no entry exists for a partition at all, {@link #watermarkFor(int, long)}
 * returns empty — GC must skip that partition rather than wipe everything.
 */
public class PartitionWatermarks {

    private final ConcurrentMap<Integer, PartitionState> byPartition = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    private record Entry(long minSeq, long lastSeenMs) {}

    private static final class PartitionState {
        final Map<String, Entry> byNode = new HashMap<>();
    }

    public PartitionWatermarks() {
        this(System::currentTimeMillis);
    }

    /** Inject a clock — used by tests to deterministically age entries. */
    public PartitionWatermarks(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Record a peer's latest gossiped state for one partition. The receiver
     * timestamps the entry against its own clock; any timestamp provided by
     * the sender on the wire is ignored.
     */
    public void update(String nodeId, int partition, long minSeq) {
        long now = clock.getAsLong();
        PartitionState state = byPartition.computeIfAbsent(partition, k -> new PartitionState());
        synchronized (state) {
            state.byNode.put(nodeId, new Entry(minSeq, now));
        }
    }

    /**
     * Compute the current global watermark for a partition, evicting entries
     * whose {@code lastSeenMs} is older than {@code staleAfterMs} relative to
     * the receiver's clock. Returns empty if no fresh entries exist.
     */
    public OptionalLong watermarkFor(int partition, long staleAfterMs) {
        PartitionState state = byPartition.get(partition);
        if (state == null) return OptionalLong.empty();
        long cutoff = clock.getAsLong() - staleAfterMs;
        long min = Long.MAX_VALUE;
        boolean any = false;
        synchronized (state) {
            Iterator<Map.Entry<String, Entry>> it = state.byNode.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Entry> e = it.next();
                Entry entry = e.getValue();
                if (entry.lastSeenMs() < cutoff) {
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
