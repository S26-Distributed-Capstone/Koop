package com.github.koop.common.pubsub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryPubSub implements PubSub {
    private PubSubListener listener;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();

    /** Per-topic ordered message log (append-only). */
    private final Map<String, List<byte[]>> topicLogs = new ConcurrentHashMap<>();

    /** Per-(consumerGroupId, topic) committed offset. */
    private final Map<String, AtomicLong> groupOffsets = new ConcurrentHashMap<>();

    /** Tracks which topics have a consumer group association. */
    private final Map<String, String> topicToGroup = new ConcurrentHashMap<>();

    @Override
    public void pub(String topic, byte[] message) {
        // Always append to the topic log, regardless of subscriptions
        topicLogs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(message);

        // Deliver to live listeners if subscribed
        if (listener != null && subscribedTopics.contains(topic)) {
            long offset = topicLogs.get(topic).size() - 1;
            listener.onMessage(topic, offset, message);

            // Advance consumer group offset if applicable
            String groupKey = topicToGroup.get(topic);
            if (groupKey != null) {
                groupOffsets.computeIfAbsent(groupKey, k -> new AtomicLong(0))
                        .updateAndGet(current -> Math.max(current, offset + 1));
            }
        }
    }

    @Override
    public void sub(String topic) {
        subscribedTopics.add(topic);
    }

    @Override
    public void drop(String topic) {
        subscribedTopics.remove(topic);
        // Don't remove group offsets — they must survive reconnection
        topicToGroup.remove(topic);
    }

    @Override
    public void start(PubSubListener listener) {
        this.listener = listener;
    }

    @Override
    public List<byte[]> pollBacklog(String topic) {
        String groupKey = topicToGroup.get(topic);
        List<byte[]> log = topicLogs.getOrDefault(topic, Collections.emptyList());

        if (log.isEmpty()) {
            return Collections.emptyList();
        }

        long fromOffset;
        if (groupKey != null) {
            fromOffset = groupOffsets.computeIfAbsent(groupKey, k -> new AtomicLong(0)).get();
        } else {
            // No consumer group — return everything from the beginning
            fromOffset = 0;
        }

        if (fromOffset >= log.size()) {
            return Collections.emptyList();
        }

        List<byte[]> backlog = new ArrayList<>(log.subList((int) fromOffset, log.size()));

        // Advance the consumer group offset past the returned messages
        if (groupKey != null) {
            groupOffsets.get(groupKey).set(log.size());
        }

        return backlog;
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}