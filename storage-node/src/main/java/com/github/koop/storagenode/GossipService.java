package com.github.koop.storagenode;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.pubsub.PubSubClient;

/**
 * Gossip-based watermarking service for the garbage-collection protocol.
 *
 * <p>The service runs on every storage node. Periodically each node publishes
 * its own per-partition lowest-in-use sequence number (drawn from
 * {@link ActiveSequenceTracker}) to a shared pub/sub topic. Every peer
 * subscribes to the same topic, building up a {@code nodeId -> (partition ->
 * seq)} table. The {@link #safeWatermark(int)} method returns the global
 * minimum across all known peers (including self) for a partition — anything
 * with a sequence number strictly less than that value can be safely garbage
 * collected.
 */
public class GossipService {

    private static final Logger logger = LogManager.getLogger(GossipService.class);

    public static final String GOSSIP_TOPIC = "gc-gossip";
    static final long DEFAULT_GOSSIP_INTERVAL_MS = 2_000;

    private final String nodeId;
    private final PubSubClient pubSub;
    private final ActiveSequenceTracker tracker;
    private final long gossipIntervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("gc-gossip").factory());

    /** Per-peer (including self) latest known per-partition lowest-in-use seq. */
    private final Map<String, Map<Integer, Long>> peerWatermarks = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public GossipService(String nodeId, PubSubClient pubSub, ActiveSequenceTracker tracker) {
        this(nodeId, pubSub, tracker, DEFAULT_GOSSIP_INTERVAL_MS);
    }

    GossipService(String nodeId, PubSubClient pubSub, ActiveSequenceTracker tracker, long gossipIntervalMs) {
        this.nodeId = nodeId;
        this.pubSub = pubSub;
        this.tracker = tracker;
        this.gossipIntervalMs = gossipIntervalMs;
    }

    public void start() {
        if (running) {
            throw new IllegalStateException("GossipService already running");
        }
        running = true;
        pubSub.sub(GOSSIP_TOPIC, (topic, offset, bytes) -> onMessage(bytes));
        long period = Math.max(gossipIntervalMs, 1);
        scheduler.scheduleWithFixedDelay(this::publishOwnWatermark, period, period, TimeUnit.MILLISECONDS);
        logger.info("GossipService started for nodeId={}, interval={}ms", nodeId, gossipIntervalMs);
    }

    public void stop() {
        running = false;
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("GossipService scheduler did not shut down within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Force a gossip publish immediately. Useful for tests. */
    public void publishOwnWatermark() {
        try {
            Map<Integer, Long> snapshot = tracker.snapshotLowestInUse();
            // Always update the local view first, even if no partitions are known
            peerWatermarks.put(nodeId, new HashMap<>(snapshot));
            if (snapshot.isEmpty()) {
                return;
            }
            GossipMessage message = new GossipMessage(nodeId, snapshot);
            pubSub.pub(GOSSIP_TOPIC, message.serialize());
        } catch (Exception e) {
            logger.error("Failed to publish gossip watermark", e);
        }
    }

    private void onMessage(byte[] bytes) {
        try {
            GossipMessage message = GossipMessage.deserialize(bytes);
            // Ignore our own gossip — we already updated the local view at publish time
            if (message.nodeId().equals(nodeId)) {
                return;
            }
            peerWatermarks.put(message.nodeId(), new HashMap<>(message.lowestInUsePerPartition()));
        } catch (Exception e) {
            logger.warn("Discarding malformed gossip message", e);
        }
    }

    /**
     * Returns the safe-deletion watermark for {@code partition}: the minimum
     * lowest-in-use sequence across every node (including self) that has
     * reported a value for this partition. Empty if no node has ever reported
     * for this partition, in which case nothing in the partition is safe to
     * delete.
     *
     * <p>Our own contribution is taken LIVE from {@link ActiveSequenceTracker}
     * rather than from the (possibly stale) last-published value in
     * {@link #peerWatermarks}. Otherwise, in the window between our gossip
     * publishes, a seq we just processed locally wouldn't yet be reflected in
     * our own entry and the worker could over-collect.
     */
    public OptionalLong safeWatermark(int partition) {
        long min = Long.MAX_VALUE;
        boolean anyReporter = false;

        Long ownLive = tracker.snapshotLowestInUse().get(partition);
        if (ownLive != null) {
            anyReporter = true;
            min = ownLive;
        }

        for (Map.Entry<String, Map<Integer, Long>> entry : peerWatermarks.entrySet()) {
            if (entry.getKey().equals(nodeId)) {
                continue; // self handled above via live tracker read
            }
            Long v = entry.getValue().get(partition);
            if (v == null) {
                continue;
            }
            anyReporter = true;
            if (v < min) {
                min = v;
            }
        }
        return anyReporter ? OptionalLong.of(min) : OptionalLong.empty();
    }

    /** Test helper: number of distinct peers that have reported a watermark. */
    int knownPeerCount() {
        return peerWatermarks.size();
    }
}
