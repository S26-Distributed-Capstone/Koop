package com.github.koop.storagenode.gc;

import java.util.HashSet;
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
 * Periodically broadcasts this node's local minimum sequence number per owned
 * partition to peers, and feeds incoming gossip into {@link PartitionWatermarks}.
 *
 * <p>The local minimum for a partition is
 * {@code min(currentPartitionSeqNum, lowestSeqNumOfActiveGETs)}.
 *
 * <p>One gossip topic is used per partition (see {@link GossipTopics}). Both
 * publisher and subscriber speak {@link WatermarkGossipMessage}.
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

    private final Set<Integer> subscribedTopics = new HashSet<>();
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
     * Compute and publish gossip for every currently-owned partition, and ensure
     * we're subscribed to the gossip topic for each. Public to allow tests to
     * drive a deterministic tick without waiting on the scheduler.
     */
    public synchronized void tick() throws Exception {
        Set<Integer> owned = ownedPartitions.apply(0);
        for (Integer partition : owned) {
            ensureSubscribed(partition);

            long maxSeq = db.getMaxSeqNum(partition);
            OptionalLong activeMin = activeReads.minActiveSeq(partition);
            long localMin = activeMin.isPresent() ? Math.min(maxSeq, activeMin.getAsLong()) : maxSeq;

            WatermarkGossipMessage msg = new WatermarkGossipMessage(
                    nodeId, partition, localMin, System.currentTimeMillis());
            pubSubClient.pub(GossipTopics.forPartition(partition), msg.serialize());

            // Also record locally — receivers include the sender in the minimum.
            watermarks.update(nodeId, partition, localMin, msg.timestampMs());
        }
    }

    private void tickQuiet() {
        try {
            tick();
        } catch (Exception e) {
            logger.warn("Gossip tick failed: {}", e.getMessage(), e);
        }
    }

    private void ensureSubscribed(int partition) {
        if (subscribedTopics.contains(partition)) return;
        String topic = GossipTopics.forPartition(partition);
        pubSubClient.sub(topic, (incomingTopic, offset, bytes) -> {
            try {
                WatermarkGossipMessage msg = WatermarkGossipMessage.deserialize(bytes);
                watermarks.update(msg.nodeId(), msg.partition(), msg.minSeqNum(), msg.timestampMs());
            } catch (Exception e) {
                logger.warn("Failed to process gossip on {} at {}: {}", incomingTopic, offset, e.getMessage());
            }
        });
        subscribedTopics.add(partition);
    }
}
