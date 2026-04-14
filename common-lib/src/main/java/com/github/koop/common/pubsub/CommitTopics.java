package com.github.koop.common.pubsub;

/**
 * Central definition of Kafka topic names for the storage commit protocol.
 *
 * <p>Both the Query Processor (which publishes commit messages) and the Storage
 * Node (which consumes them) must agree on the same topic name for a given
 * partition. Keeping the derivation here — in the {@code common} module — means
 * neither side hard-codes the format string independently.
 *
 * <p>Topic-per-partition design rationale:
 * <ul>
 *   <li>SNs only subscribe to the partitions they own, avoiding unnecessary
 *       message fan-out.</li>
 *   <li>Kafka preserves ordering within a topic, so commits for the same
 *       partition are processed in the order they were published.</li>
 * </ul>
 */
public final class CommitTopics {

    private CommitTopics() {}

    /**
     * Returns the Kafka topic name for commit messages belonging to the given
     * partition number.
     *
     * @param partition the partition number (non-negative)
     * @return topic name, e.g. {@code "partition-42"}
     */
    public static String forPartition(int partition) {
        return "partition-" + partition;
    }

    /**
     * Returns the Kafka topic name for bucket-level operations
     * (create bucket, delete bucket).
     *
     * <p>Bucket operations are not tied to a single partition — a bucket spans
     * all erasure sets — so they are published to a dedicated global topic that
     * every storage node subscribes to regardless of which partitions it owns.
     *
     * @return the shared bucket-operations topic name {@code "buckets"}
     */
    public static String forBucket() {
        return "buckets";
    }
}