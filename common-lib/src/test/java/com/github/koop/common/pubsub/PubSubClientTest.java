package com.github.koop.common.pubsub;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PubSubClientTest {

    private MemoryPubSub memoryPubSub;
    private PubSubClient client;

    @BeforeEach
    void setUp() {
        memoryPubSub = new MemoryPubSub();
        client = new PubSubClient(memoryPubSub);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
    }

    @Test
    void testSubAndPubReceivesMessages() {
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        String topic = "test-topic";

        // Subscribe to the topic with the updated signature
        client.sub(topic, (t, offset, msg) -> receivedMessage.set(new String(msg, StandardCharsets.UTF_8)));
        
        // Start the client (binds the listener to MemoryPubSub)
        client.start();

        // Publish a message 
        client.pub(topic, "hello".getBytes(StandardCharsets.UTF_8));

        assertNotNull(receivedMessage.get(), "Listener should have received the message");
        assertEquals("hello", receivedMessage.get());
    }

    @Test
    void testMultipleSubscribersOnSameTopic() {
        AtomicInteger listener1Count = new AtomicInteger(0);
        AtomicInteger listener2Count = new AtomicInteger(0);
        String topic = "broadcast-topic";

        client.sub(topic, (t, offset, msg) -> listener1Count.incrementAndGet());
        client.sub(topic, (t, offset, msg) -> listener2Count.incrementAndGet());
        client.start();

        client.pub(topic, "msg1".getBytes());
        client.pub(topic, "msg2".getBytes());

        assertEquals(2, listener1Count.get(), "Listener 1 should receive 2 messages");
        assertEquals(2, listener2Count.get(), "Listener 2 should receive 2 messages");
    }

    @Test
    void testUnrelatedTopicsAreIgnored() {
        AtomicInteger topicACount = new AtomicInteger(0);
        AtomicInteger topicBCount = new AtomicInteger(0);
        
        client.sub("topic-A", (t, offset, msg) -> topicACount.incrementAndGet());
        client.sub("topic-B", (t, offset, msg) -> topicBCount.incrementAndGet());
        client.start();

        // Publish only to topic A
        client.pub("topic-A", "hello".getBytes());

        assertEquals(1, topicACount.get(), "Listener for topic A should be triggered");
        assertEquals(0, topicBCount.get(), "Listener for topic B should not be triggered");
    }

    @Test
    void testListenerExceptionDoesNotHaltOtherListeners() {
        AtomicInteger successListenerCount = new AtomicInteger(0);
        String topic = "fault-tolerant-topic";

        // First listener throws an exception
        client.sub(topic, (t, offset, msg) -> {
            throw new RuntimeException("Simulated listener failure");
        });

        // Second listener should still get executed
        client.sub(topic, (t, offset, msg) -> successListenerCount.incrementAndGet());
        
        client.start();

        assertDoesNotThrow(() -> {
            client.pub(topic, "msg".getBytes());
        }, "Exception in a listener should be caught and not bubble up to the publisher");

        assertEquals(1, successListenerCount.get(), "Subsequent listeners should still be triggered");
    }
    
    @Test
    void testNoListenersForTopicDoesNotThrow() {
        client.start();
        
        assertDoesNotThrow(() -> {
            client.pub("empty-topic", "data".getBytes());
        }, "Publishing to a topic with no subscribers should not throw an exception");
    }

    @Test
    void testOffsetsIncrementCorrectly() {
        AtomicLong lastOffset = new AtomicLong(-1);
        String topic = "offset-topic";

        client.sub(topic, (t, offset, msg) -> lastOffset.set(offset));
        client.start();

        client.pub(topic, "msg1".getBytes());
        assertEquals(0, lastOffset.get(), "First message should have offset 0");

        client.pub(topic, "msg2".getBytes());
        assertEquals(1, lastOffset.get(), "Second message should have offset 1");
    }

    @Test
    void testDropTopicStopsMessages() {
        AtomicInteger messageCount = new AtomicInteger(0);
        String topic = "drop-topic";

        client.sub(topic, (t, offset, msg) -> messageCount.incrementAndGet());
        client.start();

        client.pub(topic, "msg1".getBytes());
        assertEquals(1, messageCount.get(), "Listener should receive the first message");

        // Drop the topic
        client.drop(topic);

        // Publish another message
        client.pub(topic, "msg2".getBytes());

        // The count should remain 1 because the subscription was dropped
        assertEquals(1, messageCount.get(), "Listener should not receive messages after the topic is dropped");
    }
}