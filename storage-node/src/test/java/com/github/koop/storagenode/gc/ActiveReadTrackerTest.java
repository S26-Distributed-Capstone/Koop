package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

public class ActiveReadTrackerTest {

    @Test
    public void emptyTrackerHasNoMinimum() {
        ActiveReadTracker t = new ActiveReadTracker();
        assertFalse(t.minActiveSeq(1).isPresent());
    }

    @Test
    public void beginRecordsMinimumPerPartition() {
        ActiveReadTracker t = new ActiveReadTracker();
        try (var h1 = t.begin(1, 100L);
             var h2 = t.begin(1, 50L);
             var h3 = t.begin(2, 200L)) {
            assertEquals(OptionalLong.of(50L), t.minActiveSeq(1));
            assertEquals(OptionalLong.of(200L), t.minActiveSeq(2));
        }
        assertFalse(t.minActiveSeq(1).isPresent());
        assertFalse(t.minActiveSeq(2).isPresent());
    }

    @Test
    public void closingLowestActiveRaisesMinimum() {
        ActiveReadTracker t = new ActiveReadTracker();
        var low = t.begin(1, 10L);
        var high = t.begin(1, 20L);
        assertEquals(OptionalLong.of(10L), t.minActiveSeq(1));
        low.close();
        assertEquals(OptionalLong.of(20L), t.minActiveSeq(1));
        high.close();
        assertFalse(t.minActiveSeq(1).isPresent());
    }

    @Test
    public void concurrentReadsOfSameSeqAreRefCounted() {
        ActiveReadTracker t = new ActiveReadTracker();
        var a = t.begin(1, 42L);
        var b = t.begin(1, 42L);
        a.close();
        assertTrue(t.minActiveSeq(1).isPresent(), "still one reader on seq=42");
        b.close();
        assertFalse(t.minActiveSeq(1).isPresent());
    }

    @Test
    public void pruneEvictsExpiredLeases() {
        AtomicLong clock = new AtomicLong(0L);
        ActiveReadTracker t = new ActiveReadTracker(clock::get,
                /*maxLeaseMs=*/1_000L, /*pruneIntervalMs=*/0L);

        // Open a handle at t=0 and never close it — simulates a leaked lease.
        var leaked = t.begin(1, 100L);
        // Open a second handle at t=500 and close it normally.
        clock.set(500L);
        var normal = t.begin(1, 50L);
        normal.close();

        // Advance past the TTL on the leaked handle; normal was already cleaned up.
        clock.set(1_500L);
        int evicted = t.prune();

        assertEquals(1, evicted, "exactly the leaked handle is force-evicted");
        assertFalse(t.minActiveSeq(1).isPresent(),
                "watermark unblocked once leaked handle's refcount is decremented");
        assertEquals(0, t.liveHandleCount());

        // The owner of the leaked handle eventually realises and calls close().
        // Must be idempotent — no double decrement, no exception.
        leaked.close();
        assertFalse(t.minActiveSeq(1).isPresent());
    }

    @Test
    public void pruneDoesNotEvictFreshLeases() {
        AtomicLong clock = new AtomicLong(0L);
        ActiveReadTracker t = new ActiveReadTracker(clock::get,
                /*maxLeaseMs=*/10_000L, /*pruneIntervalMs=*/0L);

        try (var h = t.begin(1, 7L)) {
            clock.set(500L); // well within TTL
            assertEquals(0, t.prune());
            assertEquals(OptionalLong.of(7L), t.minActiveSeq(1));
        }
    }

    @Test
    public void pruneIsPerHandleEvenAtSameSeqNum() {
        AtomicLong clock = new AtomicLong(0L);
        ActiveReadTracker t = new ActiveReadTracker(clock::get,
                /*maxLeaseMs=*/1_000L, /*pruneIntervalMs=*/0L);

        var first = t.begin(1, 42L);            // t=0
        clock.set(2_000L);
        var second = t.begin(1, 42L);           // t=2000, still fresh

        // first is expired (age=2000ms > 1000ms TTL), second is fresh (age=0).
        int evicted = t.prune();
        assertEquals(1, evicted);
        assertTrue(t.minActiveSeq(1).isPresent(), "second handle still pins seq=42");

        second.close();
        assertFalse(t.minActiveSeq(1).isPresent());
        first.close(); // no-op — already evicted
    }
}
