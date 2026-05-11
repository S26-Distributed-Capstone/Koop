package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;

class GossipServiceTest {

    private PubSubClient pubSub;
    private GossipService gossipA;
    private GossipService gossipB;

    @AfterEach
    void tearDown() {
        if (gossipA != null) gossipA.stop();
        if (gossipB != null) gossipB.stop();
    }

    @Test
    void gossipMessageRoundTrip() {
        var original = new GossipMessage("node-1",
                java.util.Map.of(1, 100L, 2, 200L, 7, 13L));
        GossipMessage rt = GossipMessage.deserialize(original.serialize());
        assertEquals("node-1", rt.nodeId());
        assertEquals(java.util.Map.of(1, 100L, 2, 200L, 7, 13L), rt.lowestInUsePerPartition());
    }

    @Test
    void safeWatermarkIsEmptyBeforeAnyGossip() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        var pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();
        gossipA = new GossipService("node-a", pubSub, tracker, 1_000);
        gossipA.start();

        assertTrue(gossipA.safeWatermark(0).isEmpty());
    }

    @Test
    void safeWatermarkReflectsOwnTrackerImmediatelyAfterPublish() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(0, 50L);

        var pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();
        gossipA = new GossipService("node-a", pubSub, tracker, 60_000);
        gossipA.start();
        gossipA.publishOwnWatermark();

        OptionalLong wm = gossipA.safeWatermark(0);
        assertTrue(wm.isPresent());
        assertEquals(50L, wm.getAsLong());
    }

    @Test
    void safeWatermarkIsGlobalMinimumAcrossPeers() throws Exception {
        // Two services on a shared PubSubClient observe each other's messages
        // through that client's per-topic listener fan-out.
        pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();

        ActiveSequenceTracker trackerA = new ActiveSequenceTracker();
        ActiveSequenceTracker trackerB = new ActiveSequenceTracker();
        trackerA.recordProcessedSeq(0, 100L);
        trackerB.recordProcessedSeq(0, 50L);

        gossipA = new GossipService("node-a", pubSub, trackerA, 60_000);
        gossipB = new GossipService("node-b", pubSub, trackerB, 60_000);
        gossipA.start();
        gossipB.start();

        gossipA.publishOwnWatermark();
        gossipB.publishOwnWatermark();

        awaitTrue(() -> gossipA.knownPeerCount() >= 2 && gossipB.knownPeerCount() >= 2, 2_000);

        assertEquals(50L, gossipA.safeWatermark(0).getAsLong(),
                "Node A must see the global minimum across peers");
        assertEquals(50L, gossipB.safeWatermark(0).getAsLong(),
                "Node B must see the global minimum across peers");
    }

    @Test
    void safeWatermarkUsesLiveTrackerEvenWithoutRepublishing() {
        // A node that has just processed a new seq but hasn't gossiped yet
        // must NOT advertise a higher watermark than its current local state
        // — otherwise the GC worker could over-collect during the gossip
        // window.
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(0, 100L);

        var pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();
        gossipA = new GossipService("node-a", pubSub, tracker, 60_000);
        gossipA.start();
        gossipA.publishOwnWatermark(); // initial publish at hw=100

        // Now an active GET begins for an old version. Without re-publishing,
        // the local safe watermark must already reflect it.
        tracker.beginGet(0, 5L);
        assertEquals(5L, gossipA.safeWatermark(0).getAsLong(),
                "safeWatermark must read live tracker state, not rely on the last gossip");
    }

    @Test
    void activeGetPinsWatermarkBelowTheHighWaterMark() {
        ActiveSequenceTracker tracker = new ActiveSequenceTracker();
        tracker.recordProcessedSeq(0, 100L);
        tracker.beginGet(0, 30L);

        var pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();
        gossipA = new GossipService("node-a", pubSub, tracker, 60_000);
        gossipA.start();
        gossipA.publishOwnWatermark();

        assertEquals(30L, gossipA.safeWatermark(0).getAsLong(),
                "An active GET below the high-water mark must drag the watermark down");

        tracker.endGet(0, 30L);
        gossipA.publishOwnWatermark();
        assertEquals(100L, gossipA.safeWatermark(0).getAsLong(),
                "Watermark returns to the high-water mark once the GET completes");
    }

    private static void awaitTrue(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
        fail("condition not met within " + timeoutMs + "ms");
    }
}
