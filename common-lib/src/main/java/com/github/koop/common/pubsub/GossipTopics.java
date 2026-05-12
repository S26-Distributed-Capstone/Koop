package com.github.koop.common.pubsub;

/**
 * Naming for gossip-based watermark topics.
 *
 * <p>Gossip is multiplexed at the node level: each node publishes a single
 * batched {@code WatermarkGossipMessage} containing every owned partition's
 * local minimum to one cluster-wide topic, rather than one topic per partition.
 * This keeps broker/connection overhead independent of partition count.
 */
public final class GossipTopics {

    /** Single cluster-wide topic carrying all node-level gossip broadcasts. */
    public static final String CLUSTER_GOSSIP_TOPIC = "gossip-cluster";

    private GossipTopics() {}
}
