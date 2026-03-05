package com.github.koop.common.pubsub;

public interface PubSub extends AutoCloseable {
    void sub(String topic, PubSubListener listener);
    void pub(String topic, byte[] message);
    void start();
}
