package com.github.koop.storagenode.gc;

import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tracks in-flight GET requests grouped by partition.
 *
 * <p>Each call to {@link #begin(int, long)} records that a client is currently
 * reading version {@code seqNum} of an object in {@code partition}. The returned
 * handle must be closed when the read completes so the entry is removed.
 *
 * <p>{@link #minActiveSeq(int)} returns the lowest sequence number currently
 * being read in that partition, used by the gossip-based GC watermark.
 *
 * <h2>TTL leases</h2>
 * Every active handle is timestamped at creation. A background pruner sweeps
 * the set and forcibly evicts any handle older than {@code maxLeaseMs},
 * decrementing the per-partition reference count. This prevents permanent
 * pinning of the watermark if a caller forgets to close the handle (e.g. a
 * server-side bug that bypasses try-with-resources, or a client socket that
 * never breaks cleanly). After force-eviction, subsequent {@code close()} calls
 * on the same handle are no-ops, so cleanup that does eventually arrive can
 * still run without double-decrementing.
 *
 * <p>For best results the network layer should also tie {@code close()} to
 * socket-disconnect detection — the {@code handleGet} response path wraps the
 * stream in try-with-resources so an {@code IOException} from a dropped client
 * connection unwinds the handle promptly without waiting for the TTL.
 */
public class ActiveReadTracker {

    private static final Logger logger = LogManager.getLogger(ActiveReadTracker.class);

    private final ConcurrentMap<Integer, PartitionState> byPartition = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, HandleEntry> liveHandles = new ConcurrentHashMap<>();
    private final AtomicLong nextHandleId = new AtomicLong();
    private final LongSupplier clock;
    private final long maxLeaseMs;
    private final long pruneIntervalMs;
    private ScheduledExecutorService scheduler;
    private boolean running = false;

    private record HandleEntry(int partition, long seqNum, long startedAtMs) {}

    private static final class PartitionState {
        // seqNum -> reference count
        final TreeMap<Long, Integer> counts = new TreeMap<>();
    }

    /**
     * No-pruner constructor (infinite lease). Useful for unit tests that drive
     * begin/close explicitly and do not want a background thread.
     */
    public ActiveReadTracker() {
        this(System::currentTimeMillis, Long.MAX_VALUE, 0L);
    }

    /**
     * @param clock              source of "now" used for lease bookkeeping
     * @param maxLeaseMs         maximum acceptable duration a handle may remain
     *                           open before being forcibly evicted; pass
     *                           {@link Long#MAX_VALUE} to disable
     * @param pruneIntervalMs    how often the background pruner runs; 0 disables
     *                           the scheduler (tests may still call {@link #prune()})
     */
    public ActiveReadTracker(LongSupplier clock, long maxLeaseMs, long pruneIntervalMs) {
        this.clock = clock;
        this.maxLeaseMs = maxLeaseMs;
        this.pruneIntervalMs = pruneIntervalMs;
    }

    public synchronized void start() {
        if (running) return;
        if (pruneIntervalMs <= 0 || maxLeaseMs == Long.MAX_VALUE) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("active-read-pruner-").factory());
        scheduler.scheduleAtFixedRate(this::pruneQuiet,
                pruneIntervalMs, pruneIntervalMs, TimeUnit.MILLISECONDS);
        running = true;
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        running = false;
    }

    public Handle begin(int partition, long seqNum) {
        long id = nextHandleId.incrementAndGet();
        long now = clock.getAsLong();
        liveHandles.put(id, new HandleEntry(partition, seqNum, now));
        increment(partition, seqNum);
        return new Handle(id);
    }

    private void increment(int partition, long seqNum) {
        PartitionState state = byPartition.computeIfAbsent(partition, k -> new PartitionState());
        synchronized (state) {
            state.counts.merge(seqNum, 1, Integer::sum);
        }
    }

    private void decrement(int partition, long seqNum) {
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

    /**
     * Evict every handle whose lease has exceeded {@code maxLeaseMs}. Returns
     * the number of handles forcibly closed. Safe to call concurrently with
     * {@link #begin(int, long)} and {@link Handle#close()}.
     */
    public int prune() {
        if (maxLeaseMs == Long.MAX_VALUE) return 0;
        long cutoff = clock.getAsLong() - maxLeaseMs;
        int evicted = 0;
        Iterator<Map.Entry<Long, HandleEntry>> it = liveHandles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, HandleEntry> e = it.next();
            HandleEntry entry = e.getValue();
            if (entry.startedAtMs() <= cutoff) {
                it.remove();
                decrement(entry.partition(), entry.seqNum());
                evicted++;
            }
        }
        if (evicted > 0) {
            logger.warn("Pruned {} expired GET lease(s) older than {}ms", evicted, maxLeaseMs);
        }
        return evicted;
    }

    private void pruneQuiet() {
        try {
            prune();
        } catch (Exception e) {
            logger.warn("Active-read pruning sweep failed: {}", e.getMessage(), e);
        }
    }

    /** Snapshot the number of live handles. Test/observability helper. */
    public int liveHandleCount() {
        return liveHandles.size();
    }

    public final class Handle implements AutoCloseable {
        private final long id;

        Handle(long id) {
            this.id = id;
        }

        /**
         * Releases the lease. Idempotent: if a background pruner has already
         * force-closed this handle, the call is a no-op (no double decrement).
         */
        @Override
        public void close() {
            HandleEntry e = liveHandles.remove(id);
            if (e != null) {
                decrement(e.partition(), e.seqNum());
            }
        }
    }
}
