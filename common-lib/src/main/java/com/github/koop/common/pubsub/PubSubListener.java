package com.github.koop.common.pubsub;

public interface PubSubListener {
    void onMessage(String topic, long offset, byte[] message);
}
