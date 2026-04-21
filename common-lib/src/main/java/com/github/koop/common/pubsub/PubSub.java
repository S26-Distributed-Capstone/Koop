package com.github.koop.common.pubsub;

import java.util.List;

public interface PubSub extends AutoCloseable {
    void pub(String topic, byte[] message);

    void sub(String topic);

    /**
     * Subscribe to a topic using a consumer group ID. When a consumer with the
     * same {@code consumerGroupId} reconnects, consumption resumes from the
     * offset where it previously left off.
     *
     * @param topic           the topic to subscribe to
     * @param consumerGroupId identifier for the consumer group
     */
    void sub(String topic, String consumerGroupId);

    void drop(String topic);

    void start(PubSubListener listener);

    /**
     * Synchronously drain all pending messages for the given topic from the
     * consumer's current offset to the head of the topic log. Advances the
     * consumer group offset past the returned messages.
     *
     * <p>Used during startup repair mode to catch up on missed messages.
     *
     * @param topic the topic to poll
     * @return an ordered list of raw message payloads; empty if no backlog
     */
    List<byte[]> pollBacklog(String topic);
}