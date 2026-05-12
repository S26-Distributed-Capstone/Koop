package com.github.koop.common.pubsub;

/**
 * Naming for gossip-based watermark topics.
 *
 * <p>Each partition has its own gossip topic so that storage nodes only receive
 * watermark updates for partitions they actually serve.
 */
public final class GossipTopics {

    private GossipTopics() {}

    /**
     * Returns the gossip topic name for the partition's watermark traffic.
     *
     * @param partition partition number (non-negative)
     * @return topic name, e.g. {@code "gossip-partition-42"}
     */
    public static String forPartition(int partition) {
        return "gossip-partition-" + partition;
    }
}
