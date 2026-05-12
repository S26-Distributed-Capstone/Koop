package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

public class PartitionWatermarksTest {

    @Test
    public void emptyPartitionHasNoWatermark() {
        PartitionWatermarks w = new PartitionWatermarks();
        assertFalse(w.watermarkFor(0, 60_000L).isPresent());
    }

    @Test
    public void watermarkIsMinOverFreshNodes() {
        PartitionWatermarks w = new PartitionWatermarks();
        long now = System.currentTimeMillis();
        w.update("A", 1, 10L, now);
        w.update("B", 1, 5L, now);
        w.update("C", 1, 25L, now);
        assertEquals(OptionalLong.of(5L), w.watermarkFor(1, 60_000L));
    }

    @Test
    public void staleEntriesAreEvictedAndIgnored() {
        PartitionWatermarks w = new PartitionWatermarks();
        long now = System.currentTimeMillis();
        // Stale: way before cutoff
        w.update("stale", 1, 1L, now - 120_000L);
        // Fresh
        w.update("fresh", 1, 50L, now);

        // 60s staleness: only "fresh" counts → watermark = 50
        assertEquals(OptionalLong.of(50L), w.watermarkFor(1, 60_000L));
    }

    @Test
    public void allStaleReturnsEmpty() {
        PartitionWatermarks w = new PartitionWatermarks();
        long old = System.currentTimeMillis() - 120_000L;
        w.update("x", 1, 5L, old);
        assertFalse(w.watermarkFor(1, 30_000L).isPresent());
    }

    @Test
    public void laterUpdateOverridesEarlier() {
        PartitionWatermarks w = new PartitionWatermarks();
        long now = System.currentTimeMillis();
        w.update("A", 1, 10L, now);
        w.update("A", 1, 20L, now + 1);
        assertEquals(OptionalLong.of(20L), w.watermarkFor(1, 60_000L));
    }
}
