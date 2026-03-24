package com.github.koop.common.pubsub;

public interface PubSub extends AutoCloseable {
    void pub(String topic, byte[] message);

    void sub(String topic);

    void drop(String topic);

    void start(PubSubListener listener);
}