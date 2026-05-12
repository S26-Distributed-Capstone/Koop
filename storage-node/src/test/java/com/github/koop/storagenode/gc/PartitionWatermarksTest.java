package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * Staleness is computed against the receiver's clock — entries are stamped at
 * receive time, not by the sender — so tests inject an {@code AtomicLong}
 * clock and advance it to simulate aging.
 */
public class PartitionWatermarksTest {

    @Test
    public void emptyPartitionHasNoWatermark() {
        PartitionWatermarks w = new PartitionWatermarks();
        assertFalse(w.watermarkFor(0, 60_000L).isPresent());
    }

    @Test
    public void watermarkIsMinOverFreshNodes() {
        AtomicLong now = new AtomicLong(1_000L);
        PartitionWatermarks w = new PartitionWatermarks(now::get);
        w.update("A", 1, 10L);
        w.update("B", 1, 5L);
        w.update("C", 1, 25L);
        assertEquals(OptionalLong.of(5L), w.watermarkFor(1, 60_000L));
    }

    @Test
    public void staleEntriesAreEvictedAndIgnored() {
        AtomicLong now = new AtomicLong(0L);
        PartitionWatermarks w = new PartitionWatermarks(now::get);

        // Stale insert at t=0
        w.update("stale", 1, 1L);
        // Fresh insert at t=120_000
        now.set(120_000L);
        w.update("fresh", 1, 50L);

        // Querying at t=120_000 with 60s staleness: cutoff=60_000.
        // stale's lastSeen=0 < cutoff → evicted; fresh's lastSeen=120_000 → kept.
        assertEquals(OptionalLong.of(50L), w.watermarkFor(1, 60_000L));
    }

    @Test
    public void allStaleReturnsEmpty() {
        AtomicLong now = new AtomicLong(0L);
        PartitionWatermarks w = new PartitionWatermarks(now::get);
        w.update("x", 1, 5L);
        // Jump the clock far past the staleness window.
        now.set(120_000L);
        assertFalse(w.watermarkFor(1, 30_000L).isPresent());
    }

    @Test
    public void laterUpdateOverridesEarlier() {
        AtomicLong now = new AtomicLong(1_000L);
        PartitionWatermarks w = new PartitionWatermarks(now::get);
        w.update("A", 1, 10L);
        now.set(1_001L);
        w.update("A", 1, 20L);
        assertEquals(OptionalLong.of(20L), w.watermarkFor(1, 60_000L));
    }

    @Test
    public void receiverIgnoresSenderClockSkew() {
        // Simulates a peer whose clock is wildly out of sync. The sender's
        // wire timestamp is irrelevant — only the receiver's local clock
        // controls when the entry goes stale. Since update() ignores the
        // sender timestamp entirely, this is essentially the same test as
        // "fresh on receive" but framed for the clock-drift property.
        AtomicLong now = new AtomicLong(1_000_000L);
        PartitionWatermarks w = new PartitionWatermarks(now::get);

        // Peer with badly skewed clock pushes an update. The receiver stamps it
        // at now=1_000_000 regardless of what timestamp the wire said.
        w.update("skewed-peer", 1, 42L);

        // 10ms later: still well within any reasonable staleness window.
        now.set(1_000_010L);
        assertEquals(OptionalLong.of(42L), w.watermarkFor(1, 60_000L));
    }
}
