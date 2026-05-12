package com.github.koop.storagenode.gc;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.messages.WatermarkGossipMessage;
import com.github.koop.common.pubsub.GossipTopics;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;

/**
 * Periodically broadcasts this node's local minimum sequence number for every
 * owned partition to peers, batched into a single message, and feeds incoming
 * gossip into {@link PartitionWatermarks}.
 *
 * <p>The local minimum for a partition is
 * {@code min(currentPartitionSeqNum, lowestSeqNumOfActiveGETs)}.
 *
 * <p>Gossip is multiplexed at the node level: one message per node per tick on
 * {@link GossipTopics#CLUSTER_GOSSIP_TOPIC}, carrying a {@code Map<Integer, Long>}
 * of partition → local min. This keeps traffic O(nodes) rather than O(partitions),
 * so clusters with tens of thousands of partitions don't drown the broker in
 * tiny per-partition messages.
 */
public class GossipService {

    private static final Logger logger = LogManager.getLogger(GossipService.class);

    private final String nodeId;
    private final Database db;
    private final ActiveReadTracker activeReads;
    private final PartitionWatermarks watermarks;
    private final PubSubClient pubSubClient;
    private final IntFunction<Set<Integer>> ownedPartitions;
    private final long gossipIntervalMs;
    private final ScheduledExecutorService scheduler;

    private volatile boolean subscribed = false;
    private volatile boolean running = false;

    public GossipService(String nodeId,
                         Database db,
                         ActiveReadTracker activeReads,
                         PartitionWatermarks watermarks,
                         PubSubClient pubSubClient,
                         IntFunction<Set<Integer>> ownedPartitions,
                         long gossipIntervalMs) {
        this.nodeId = nodeId;
        this.db = db;
        this.activeReads = activeReads;
        this.watermarks = watermarks;
        this.pubSubClient = pubSubClient;
        this.ownedPartitions = ownedPartitions;
        this.gossipIntervalMs = gossipIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("gossip-" + nodeId + "-").factory());
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        scheduler.scheduleAtFixedRate(this::tickQuiet,
                gossipIntervalMs, gossipIntervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        running = false;
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Compute and publish a single batched gossip message covering every
     * currently-owned partition. Public to let tests drive deterministic ticks
     * without waiting on the scheduler.
     */
    public synchronized void tick() throws Exception {
        ensureSubscribed();

        Set<Integer> owned = ownedPartitions.apply(0);
        if (owned.isEmpty()) return;

        Map<Integer, Long> partitionMins = new HashMap<>(owned.size() * 2);
        for (Integer partition : owned) {
            long maxSeq = db.getMaxSeqNum(partition);
            OptionalLong activeMin = activeReads.minActiveSeq(partition);
            long localMin = activeMin.isPresent() ? Math.min(maxSeq, activeMin.getAsLong()) : maxSeq;
            partitionMins.put(partition, localMin);
        }

        long now = System.currentTimeMillis();
        WatermarkGossipMessage msg = new WatermarkGossipMessage(nodeId, partitionMins, now);
        pubSubClient.pub(GossipTopics.CLUSTER_GOSSIP_TOPIC, msg.serialize());

        // Self-record — receivers include the sender in the minimum.
        for (Map.Entry<Integer, Long> e : partitionMins.entrySet()) {
            watermarks.update(nodeId, e.getKey(), e.getValue(), now);
        }
    }

    private void tickQuiet() {
        try {
            tick();
        } catch (Exception e) {
            logger.warn("Gossip tick failed: {}", e.getMessage(), e);
        }
    }

    private void ensureSubscribed() {
        if (subscribed) return;
        pubSubClient.sub(GossipTopics.CLUSTER_GOSSIP_TOPIC, (incomingTopic, offset, bytes) -> {
            try {
                WatermarkGossipMessage msg = WatermarkGossipMessage.deserialize(bytes);
                for (Map.Entry<Integer, Long> e : msg.partitionMins().entrySet()) {
                    watermarks.update(msg.nodeId(), e.getKey(), e.getValue(), msg.timestampMs());
                }
            } catch (Exception e) {
                logger.warn("Failed to process gossip on {} at {}: {}", incomingTopic, offset, e.getMessage());
            }
        });
        subscribed = true;
    }
}
