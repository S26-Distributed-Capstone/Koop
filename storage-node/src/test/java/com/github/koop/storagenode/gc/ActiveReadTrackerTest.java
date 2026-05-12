package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.OptionalLong;

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
}
