package com.github.koop.common.pubsub;

import java.util.function.BiConsumer;

public class MemoryPubSub implements PubSub {
    private BiConsumer<String, byte[]> listener;

    @Override
    public void pub(String topic, byte[] message) {
        if (listener != null) {
            listener.accept(topic, message);
        }
    }

    @Override
    public void start(BiConsumer<String, byte[]> listener) {
        this.listener = listener;
    }

    @Override
    public void close() {
        // No resources to clean up
    }
    
}
