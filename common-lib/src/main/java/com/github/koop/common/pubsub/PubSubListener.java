package com.github.koop.common.pubsub;

import java.util.function.Consumer;

public interface PubSubListener {
    void onMessage(String topic, long offset, byte[] message);
}
