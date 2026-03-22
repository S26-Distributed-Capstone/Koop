package com.github.koop.common.pubsub;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryPubSub implements PubSub {
    private PubSubListener listener;
    private final Map<String, AtomicLong> offsets = new ConcurrentHashMap<>();
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();

    @Override
    public void pub(String topic, byte[] message) {
        if (listener != null && subscribedTopics.contains(topic)) {
            long offset = offsets.computeIfAbsent(topic, k -> new AtomicLong(0)).getAndIncrement();
            listener.onMessage(topic, offset, message);
        }
    }

    @Override
    public void sub(String topic) {
        subscribedTopics.add(topic);
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