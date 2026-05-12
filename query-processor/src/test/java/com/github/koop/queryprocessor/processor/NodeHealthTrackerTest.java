package com.github.koop.queryprocessor.processor;

import com.github.koop.queryprocessor.processor.NodeHealthTracker.NodeHealth;
import com.github.koop.queryprocessor.processor.NodeHealthTracker.NodeStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NodeHealthTrackerTest {

    private static final InetSocketAddress NODE_A = new InetSocketAddress("10.0.0.1", 8080);
    private static final InetSocketAddress NODE_B = new InetSocketAddress("10.0.0.2", 8080);
    private static final InetSocketAddress NODE_C = new InetSocketAddress("10.0.0.3", 8080);

    private NodeHealthTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new NodeHealthTracker();
    }

    // -------------------------------------------------------------------------
    // Default / unknown node behavior
    // -------------------------------------------------------------------------

    @Test
    void unknownNode_isConsideredHealthy() {
        assertTrue(tracker.isHealthy(NODE_A),
                "Unknown node should be considered healthy (optimistic default)");
    }

    @Test
    void unknownNode_getHealth_returnsNull() {
        assertNull(tracker.getHealth(NODE_A),
                "getHealth for unknown node should return null");
    }

    // -------------------------------------------------------------------------
    // State machine: HEALTHY → SUSPECT → DOWN
    // -------------------------------------------------------------------------

    @Test
    void singleFailure_transitionsToSuspect() {
        tracker.recordFailure(NODE_A);

        NodeHealth health = tracker.getHealth(NODE_A);
        assertNotNull(health);
        assertEquals(NodeStatus.SUSPECT, health.status());
        assertEquals(1, health.consecutiveFailures());
        assertTrue(tracker.isHealthy(NODE_A),
                "SUSPECT nodes should still be considered healthy");
    }

    @Test
    void twoConsecutiveFailures_transitionsToDown() {
        tracker.recordFailure(NODE_A);
        tracker.recordFailure(NODE_A);

        NodeHealth health = tracker.getHealth(NODE_A);
        assertNotNull(health);
        assertEquals(NodeStatus.DOWN, health.status());
        assertEquals(2, health.consecutiveFailures());
        assertFalse(tracker.isHealthy(NODE_A),
                "DOWN nodes should not be considered healthy");
    }

    @Test
    void additionalFailures_stayDown() {
        tracker.recordFailure(NODE_A);
        tracker.recordFailure(NODE_A);
        tracker.recordFailure(NODE_A); // third failure

        NodeHealth health = tracker.getHealth(NODE_A);
        assertEquals(NodeStatus.DOWN, health.status());
        assertEquals(3, health.consecutiveFailures());
        assertFalse(tracker.isHealthy(NODE_A));
    }

    // -------------------------------------------------------------------------
    // Recovery: success resets to HEALTHY
    // -------------------------------------------------------------------------

    @Test
    void successFromSuspect_resetsToHealthy() {
        tracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.SUSPECT, tracker.getHealth(NODE_A).status());

        tracker.recordSuccess(NODE_A);

        NodeHealth health = tracker.getHealth(NODE_A);
        assertEquals(NodeStatus.HEALTHY, health.status());
        assertEquals(0, health.consecutiveFailures());
        assertTrue(tracker.isHealthy(NODE_A));
    }

    @Test
    void successFromDown_resetsToHealthy() {
        tracker.recordFailure(NODE_A);
        tracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.DOWN, tracker.getHealth(NODE_A).status());

        tracker.recordSuccess(NODE_A);

        NodeHealth health = tracker.getHealth(NODE_A);
        assertEquals(NodeStatus.HEALTHY, health.status());
        assertEquals(0, health.consecutiveFailures());
        assertTrue(tracker.isHealthy(NODE_A));
    }

    @Test
    void successOnUnknownNode_registersAsHealthy() {
        tracker.recordSuccess(NODE_A);

        NodeHealth health = tracker.getHealth(NODE_A);
        assertNotNull(health);
        assertEquals(NodeStatus.HEALTHY, health.status());
        assertEquals(0, health.consecutiveFailures());
    }

    @Test
    void successOnAlreadyHealthyNode_isNoOp() {
        tracker.recordSuccess(NODE_A);
        Instant firstTime = tracker.getHealth(NODE_A).lastUpdateTime();

        tracker.recordSuccess(NODE_A);
        // Should not create a new record since already healthy with 0 failures
        NodeHealth health = tracker.getHealth(NODE_A);
        assertEquals(NodeStatus.HEALTHY, health.status());
        assertEquals(0, health.consecutiveFailures());
    }

    // -------------------------------------------------------------------------
    // Custom failure threshold
    // -------------------------------------------------------------------------

    @Test
    void customThreshold_requiresMoreFailuresForDown() {
        NodeHealthTracker customTracker = new NodeHealthTracker(3, Clock.systemUTC());

        customTracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.SUSPECT, customTracker.getHealth(NODE_A).status());

        customTracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.SUSPECT, customTracker.getHealth(NODE_A).status());

        customTracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.DOWN, customTracker.getHealth(NODE_A).status());
    }

    @Test
    void invalidThreshold_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> new NodeHealthTracker(0, Clock.systemUTC()));
    }

    // -------------------------------------------------------------------------
    // getHealthyNodes filtering
    // -------------------------------------------------------------------------

    @Test
    void getHealthyNodes_excludesDownNodes() {
        tracker.recordSuccess(NODE_A); // HEALTHY
        tracker.recordFailure(NODE_B); // SUSPECT
        tracker.recordFailure(NODE_C);
        tracker.recordFailure(NODE_C); // DOWN

        Set<InetSocketAddress> healthy = tracker.getHealthyNodes(List.of(NODE_A, NODE_B, NODE_C));

        assertTrue(healthy.contains(NODE_A), "HEALTHY node should be included");
        assertTrue(healthy.contains(NODE_B), "SUSPECT node should be included");
        assertFalse(healthy.contains(NODE_C), "DOWN node should be excluded");
    }

    @Test
    void getHealthyNodes_includesUnknownNodes() {
        InetSocketAddress unknown = new InetSocketAddress("10.0.0.99", 8080);
        Set<InetSocketAddress> healthy = tracker.getHealthyNodes(List.of(unknown));

        assertTrue(healthy.contains(unknown),
                "Unknown nodes should be included (optimistic default)");
    }

    @Test
    void getHealthyNodes_emptyInput_returnsEmpty() {
        Set<InetSocketAddress> healthy = tracker.getHealthyNodes(List.of());
        assertTrue(healthy.isEmpty());
    }

    // -------------------------------------------------------------------------
    // getAllTrackedNodes
    // -------------------------------------------------------------------------

    @Test
    void getAllTrackedNodes_returnsAllTracked() {
        tracker.recordSuccess(NODE_A);
        tracker.recordFailure(NODE_B);

        Set<InetSocketAddress> tracked = tracker.getAllTrackedNodes();
        assertEquals(Set.of(NODE_A, NODE_B), tracked);
    }

    @Test
    void getAllTrackedNodes_emptyWhenNoActivity() {
        assertTrue(tracker.getAllTrackedNodes().isEmpty());
    }

    // -------------------------------------------------------------------------
    // getStatusCounts
    // -------------------------------------------------------------------------

    @Test
    void getStatusCounts_correctCounts() {
        tracker.recordSuccess(NODE_A); // HEALTHY
        tracker.recordFailure(NODE_B); // SUSPECT
        tracker.recordFailure(NODE_C);
        tracker.recordFailure(NODE_C); // DOWN

        Map<NodeStatus, Long> counts = tracker.getStatusCounts();
        assertEquals(1L, counts.getOrDefault(NodeStatus.HEALTHY, 0L));
        assertEquals(1L, counts.getOrDefault(NodeStatus.SUSPECT, 0L));
        assertEquals(1L, counts.getOrDefault(NodeStatus.DOWN, 0L));
    }

    // -------------------------------------------------------------------------
    // Clock injection
    // -------------------------------------------------------------------------

    @Test
    void timestamps_useInjectedClock() {
        Instant fixedTime = Instant.parse("2026-01-01T00:00:00Z");
        Clock fixedClock = Clock.fixed(fixedTime, ZoneId.of("UTC"));
        NodeHealthTracker clockTracker = new NodeHealthTracker(2, fixedClock);

        clockTracker.recordFailure(NODE_A);
        assertEquals(fixedTime, clockTracker.getHealth(NODE_A).lastUpdateTime());
    }

    // -------------------------------------------------------------------------
    // Thread-safety under concurrent access
    // -------------------------------------------------------------------------

    @Test
    void concurrentSuccessAndFailure_noExceptions() throws InterruptedException {
        int threadCount = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 100; j++) {
                        if (idx % 2 == 0) {
                            tracker.recordFailure(NODE_A);
                        } else {
                            tracker.recordSuccess(NODE_A);
                        }
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();

        assertTrue(errors.isEmpty(), "Concurrent access should not throw: " + errors);
        // Node should be tracked regardless of final state
        assertNotNull(tracker.getHealth(NODE_A));
    }

    @Test
    void concurrentFailures_reachDownEventually() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    startLatch.await();
                    tracker.recordFailure(NODE_A);
                } catch (InterruptedException ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();

        // After 10 concurrent failures, node should definitely be DOWN
        NodeHealth health = tracker.getHealth(NODE_A);
        assertNotNull(health);
        assertEquals(NodeStatus.DOWN, health.status());
    }

    // -------------------------------------------------------------------------
    // Mixed scenario: interleaved failures and successes
    // -------------------------------------------------------------------------

    @Test
    void failureThenSuccess_thenFailureAgain_cyclesCorrectly() {
        // HEALTHY → SUSPECT
        tracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.SUSPECT, tracker.getHealth(NODE_A).status());

        // SUSPECT → DOWN
        tracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.DOWN, tracker.getHealth(NODE_A).status());

        // DOWN → HEALTHY (success resets)
        tracker.recordSuccess(NODE_A);
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(NODE_A).status());
        assertEquals(0, tracker.getHealth(NODE_A).consecutiveFailures());

        // HEALTHY → SUSPECT again (new failure cycle)
        tracker.recordFailure(NODE_A);
        assertEquals(NodeStatus.SUSPECT, tracker.getHealth(NODE_A).status());
        assertEquals(1, tracker.getHealth(NODE_A).consecutiveFailures());
    }

    @Test
    void multipleNodes_independent() {
        tracker.recordFailure(NODE_A);
        tracker.recordFailure(NODE_A); // NODE_A → DOWN

        tracker.recordSuccess(NODE_B); // NODE_B → HEALTHY

        tracker.recordFailure(NODE_C); // NODE_C → SUSPECT

        assertEquals(NodeStatus.DOWN, tracker.getHealth(NODE_A).status());
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(NODE_B).status());
        assertEquals(NodeStatus.SUSPECT, tracker.getHealth(NODE_C).status());
    }
}
