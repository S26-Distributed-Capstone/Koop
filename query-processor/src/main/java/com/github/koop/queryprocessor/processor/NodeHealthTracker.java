package com.github.koop.queryprocessor.processor;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Thread-safe tracker for storage node health status.
 *
 * <p>The QP uses this cache to avoid sending requests to nodes that are
 * known to be unreachable, eliminating per-request timeout penalties for
 * dead nodes. The cache is <em>advisory only</em> — it never reduces
 * quorum requirements, and callers should fall back to the full node list
 * when the healthy set is too small.
 *
 * <h2>State Machine</h2>
 * <pre>
 *   HEALTHY ──(1 failure)──▶ SUSPECT ──(1 more failure)──▶ DOWN
 *      ▲                                                      │
 *      └──────────────────(1 success)─────────────────────────┘
 * </pre>
 *
 * <p>SUSPECT nodes are still included in {@link #isHealthy} checks
 * (soft degradation); DOWN nodes are excluded. Any successful request
 * to a DOWN or SUSPECT node immediately promotes it back to HEALTHY.
 *
 * <p>Each QP instance maintains its own local tracker — no cross-QP
 * coordination is needed since QPs are stateless.
 */
public final class NodeHealthTracker {

    private static final Logger logger = LogManager.getLogger(NodeHealthTracker.class);

    /** Default number of consecutive failures before a node transitions to DOWN. */
    static final int DEFAULT_FAILURE_THRESHOLD = 2;

    private final int failureThreshold;
    private final Clock clock;
    private final ConcurrentHashMap<InetSocketAddress, AtomicReference<NodeHealth>> nodes = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Status enum
    // -------------------------------------------------------------------------

    /** Liveness state for a tracked storage node. */
    public enum NodeStatus {
        /** Node is responding normally. */
        HEALTHY,
        /** Node has failed once recently; still included in operations. */
        SUSPECT,
        /** Node has failed {@code failureThreshold} times consecutively; excluded from operations. */
        DOWN
    }

    // -------------------------------------------------------------------------
    // Health record
    // -------------------------------------------------------------------------

    /**
     * Immutable snapshot of a node's health at a point in time.
     *
     * @param status             current liveness state
     * @param consecutiveFailures number of consecutive probe/request failures
     * @param lastUpdateTime     wall-clock time of the last state change
     */
    public record NodeHealth(NodeStatus status, int consecutiveFailures, Instant lastUpdateTime) {
    }

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    public NodeHealthTracker() {
        this(DEFAULT_FAILURE_THRESHOLD, Clock.systemUTC());
    }

    /**
     * Full constructor for testing with injectable clock and custom threshold.
     *
     * @param failureThreshold consecutive failures required to reach DOWN
     * @param clock            clock used for timestamping state changes
     */
    public NodeHealthTracker(int failureThreshold, Clock clock) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1, got " + failureThreshold);
        }
        this.failureThreshold = failureThreshold;
        this.clock = clock;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the node is HEALTHY or SUSPECT (i.e. still
     * worth attempting). Returns {@code false} only for DOWN nodes.
     *
     * <p>Unknown nodes are assumed HEALTHY (optimistic default).
     */
    public boolean isHealthy(InetSocketAddress node) {
        AtomicReference<NodeHealth> ref = nodes.get(node);
        if (ref == null) return true; // unknown = healthy
        return ref.get().status() != NodeStatus.DOWN;
    }

    /**
     * Records a successful request or probe to the given node, promoting
     * it to HEALTHY regardless of its previous state.
     */
    public void recordSuccess(InetSocketAddress node) {
        NodeHealth healthy = new NodeHealth(NodeStatus.HEALTHY, 0, clock.instant());
        AtomicReference<NodeHealth> ref = nodes.computeIfAbsent(node, k -> new AtomicReference<>(healthy));

        NodeHealth prev;
        do {
            prev = ref.get();
            if (prev.status() == NodeStatus.HEALTHY && prev.consecutiveFailures() == 0) {
                return; // already healthy, no update needed
            }
        } while (!ref.compareAndSet(prev, healthy));

        if (prev.status() != NodeStatus.HEALTHY) {
            logger.info("Node {} recovered: {} → HEALTHY", node, prev.status());
        }
    }

    /**
     * Records a failed request or probe to the given node. Transitions:
     * <ul>
     *   <li>HEALTHY → SUSPECT (after 1 failure)</li>
     *   <li>SUSPECT → DOWN (after {@code failureThreshold} total consecutive failures)</li>
     *   <li>DOWN → DOWN (failure count continues to increment)</li>
     * </ul>
     */
    public void recordFailure(InetSocketAddress node) {
        NodeHealth initial = new NodeHealth(NodeStatus.HEALTHY, 0, clock.instant());
        AtomicReference<NodeHealth> ref = nodes.computeIfAbsent(node, k -> new AtomicReference<>(initial));

        NodeHealth prev, next;
        do {
            prev = ref.get();
            int newFailures = prev.consecutiveFailures() + 1;
            NodeStatus newStatus;
            if (newFailures >= failureThreshold) {
                newStatus = NodeStatus.DOWN;
            } else {
                newStatus = NodeStatus.SUSPECT;
            }
            next = new NodeHealth(newStatus, newFailures, clock.instant());
        } while (!ref.compareAndSet(prev, next));

        if (prev.status() != next.status()) {
            logger.info("Node {} health changed: {} → {} (consecutive failures: {})",
                    node, prev.status(), next.status(), next.consecutiveFailures());
        }
    }

    /**
     * Filters the given candidate list, returning only nodes whose status
     * is HEALTHY or SUSPECT (i.e. {@link #isHealthy} returns {@code true}).
     *
     * <p>Unknown nodes are included (optimistic default).
     */
    public Set<InetSocketAddress> getHealthyNodes(Collection<InetSocketAddress> candidates) {
        return candidates.stream()
                .filter(this::isHealthy)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the set of all nodes currently tracked by this tracker.
     * Useful for probe scheduling.
     */
    public Set<InetSocketAddress> getAllTrackedNodes() {
        return Set.copyOf(nodes.keySet());
    }

    /**
     * Returns the current health snapshot for a node, or {@code null}
     * if the node has never been tracked.
     */
    public NodeHealth getHealth(InetSocketAddress node) {
        AtomicReference<NodeHealth> ref = nodes.get(node);
        return ref != null ? ref.get() : null;
    }

    /**
     * Returns counts of nodes in each status for summary reporting.
     *
     * @return map from {@link NodeStatus} to count
     */
    public Map<NodeStatus, Long> getStatusCounts() {
        return nodes.values().stream()
                .map(ref -> ref.get().status())
                .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
    }
}
