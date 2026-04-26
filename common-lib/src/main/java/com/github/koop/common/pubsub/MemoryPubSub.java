package com.github.koop.common.pubsub;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryPubSub implements PubSub {
    private PubSubListener listener;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();

    /** Per-topic ordered message log (append-only). */
    private final Map<String, ConcurrentLinkedQueue<byte[]>> topicQueues = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();

    @Override
    public void pub(String topic, byte[] message) {
        // Always append to the topic log, regardless of subscriptions
        topicQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>()).add(message);

        // Deliver to live listeners if subscribed
        if (listener != null && subscribedTopics.contains(topic)) {
            long offset = topicOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1)).incrementAndGet();
            listener.onMessage(topic, offset, message);
        }
    }

    @Override
    public void sub(String topic) {
        subscribedTopics.add(topic);
    }

    @Override
    public void drop(String topic) {
        subscribedTopics.remove(topic);
    }

    @Override
    public void start(PubSubListener listener) {
        this.listener = listener;
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}