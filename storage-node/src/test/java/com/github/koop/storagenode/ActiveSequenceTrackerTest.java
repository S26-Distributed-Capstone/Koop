package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ActiveSequenceTrackerTest {

    @Test
    void emptyTrackerReportsNoPartitions() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        assertTrue(tracker.snapshotLowestInUse().isEmpty());
    }

    @Test
    void highWaterMarkAdvancesMonotonically() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(1, 10L);
        tracker.recordProcessedSeq(1, 30L);
        tracker.recordProcessedSeq(1, 20L); // older, must be ignored

        Map<Integer, Long> snap = tracker.snapshotLowestInUse();
        assertEquals(30L, snap.get(1).longValue());
    }

    @Test
    void activeGetDragsLowestInUseDown() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(1, 100L);
        tracker.beginGet(1, 30L);
        tracker.beginGet(1, 80L);

        assertEquals(30L, tracker.snapshotLowestInUse().get(1).longValue());

        tracker.endGet(1, 30L);
        assertEquals(80L, tracker.snapshotLowestInUse().get(1).longValue());

        tracker.endGet(1, 80L);
        assertEquals(100L, tracker.snapshotLowestInUse().get(1).longValue());
    }

    @Test
    void concurrentGetsOnSameSeqAreReferenceCounted() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(1, 100L);
        tracker.beginGet(1, 5L);
        tracker.beginGet(1, 5L); // second GET on the same version

        tracker.endGet(1, 5L);
        assertEquals(5L, tracker.snapshotLowestInUse().get(1).longValue(),
                "Version must remain pinned while another GET is still serving it");

        tracker.endGet(1, 5L);
        assertEquals(100L, tracker.snapshotLowestInUse().get(1).longValue(),
                "Pin must release once every GET on that seq has finished");
    }

    @Test
    void perPartitionTracking() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(1, 50L);
        tracker.recordProcessedSeq(2, 200L);
        tracker.beginGet(2, 75L);

        Map<Integer, Long> snap = tracker.snapshotLowestInUse();
        assertEquals(50L, snap.get(1).longValue());
        assertEquals(75L, snap.get(2).longValue());
    }
}
