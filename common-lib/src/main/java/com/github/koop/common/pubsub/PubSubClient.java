package com.github.koop.common.pubsub;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PubSubClient {
    private final PubSub pubSub;
    private final Map<String, List<PubSubListener>> listeners;

    private final static Logger logger = LogManager.getLogger(PubSubClient.class);

    public PubSubClient(PubSub pubSub) {
        this.pubSub = pubSub;
        this.listeners = new ConcurrentHashMap<>();
    }

    public void sub(String topic, PubSubListener listener) {
        this.listeners.compute(topic, (k, lst) -> {
            if (lst == null) {
                lst = new CopyOnWriteArrayList<>();
                // Tell the underlying pubsub implementation to subscribe
                // to this topic when the first listener is added.
                this.pubSub.sub(topic);
            }
            lst.add(listener);
            return lst;
        });
    }

    public void pub(String topic, byte[] message) {
        this.pubSub.pub(topic, message);
    }

    public void start() {
        this.pubSub.start((topic, offset, message) -> {
            var lst = this.listeners.get(topic);
            if (lst != null) {
                for (var listener : lst) {
                    try {
                        listener.onMessage(topic, offset, message);
                    } catch (Exception e) {
                        logger.error("Error in listener for topic {}, offset {}, error: {}", topic, offset, e.getMessage());
                    }
                }
            }
        });
    }

    public void close() throws Exception {
        this.pubSub.close();
    }
}