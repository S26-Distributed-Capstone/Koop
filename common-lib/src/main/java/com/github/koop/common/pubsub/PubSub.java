package com.github.koop.common.pubsub;

import java.util.function.BiConsumer;

public interface PubSub extends AutoCloseable {
    void pub(String topic, byte[] message);

    void sub(String topic); 
    
    void start(PubSubListener listener);
}
