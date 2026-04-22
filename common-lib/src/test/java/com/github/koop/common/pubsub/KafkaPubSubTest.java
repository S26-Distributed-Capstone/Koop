package com.github.koop.common.pubsub;

import org.junit.jupiter.api.*;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalTime;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link KafkaPubSub} using a Testcontainers-managed
 * Kafka broker. The container starts once for the class and is shared across
 * all tests.
 *
 * Run with:
 *   mvn test -pl common-lib -Dtest=KafkaPubSubTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaPubSubTest {

    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    private KafkaPubSub pubSub;
    private PubSubClient client;

    private static void log(String msg) {
        System.out.printf("[%s] %s%n", LocalTime.now().withNano(0), msg);
    }

    @BeforeAll
    void logKafkaStartup() {
        log("=== Kafka container started ===");
        log("Bootstrap servers: " + kafka.getBootstrapServers());
    }

    @BeforeEach
    void setUp(TestInfo info) {
        log("--- Setting up test: " + info.getDisplayName() + " ---");
        String groupId = "test-group-" + System.currentTimeMillis();
        log("Creating KafkaPubSub with groupId=" + groupId);
        pubSub = new KafkaPubSub(kafka.getBootstrapServers(), groupId);
        client = new PubSubClient(pubSub);
        client.start();
        log("PubSubClient started");
    }

    @AfterEach
    void tearDown(TestInfo info) throws Exception {
        log("--- Tearing down test: " + info.getDisplayName() + " ---");
        client.close();
        log("PubSubClient closed");
    }

    // ─── Tests ────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void pub_sub_roundTrip() throws InterruptedException {
        String topic = CommitTopics.forPartition(0);
        byte[] payload = "hello".getBytes();

        log("Subscribing to topic: " + topic);
        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<byte[]> received = new CopyOnWriteArrayList<>();

        client.sub(topic, (t, offset, message) -> {
            log("Message received on topic=" + t + " offset=" + offset + " payload=" + new String(message));
            received.add(message);
            latch.countDown();
        });

        log("Waiting 3s for consumer group rebalance...");
        Thread.sleep(3000);

        log("Publishing message to topic: " + topic);
        client.pub(topic, payload);

        log("Waiting up to 15s for message...");
        boolean received_ = latch.await(15, TimeUnit.SECONDS);
        log("Latch result: " + received_ + " (received " + received.size() + " messages)");

        assertTrue(received_, "Message not received within timeout");
        assertEquals(1, received.size());
        assertArrayEquals(payload, received.get(0));
        log("Test passed");
    }

    @Test
    @Order(2)
    void pub_sub_multipleMessages() throws InterruptedException {
        String topic = CommitTopics.forPartition(1);
        int messageCount = 5;

        log("Subscribing to topic: " + topic);
        CountDownLatch latch = new CountDownLatch(messageCount);
        CopyOnWriteArrayList<byte[]> received = new CopyOnWriteArrayList<>();

        client.sub(topic, (t, offset, message) -> {
            log("Message received: " + new String(message) + " (total=" + (received.size() + 1) + ")");
            received.add(message);
            latch.countDown();
        });

        log("Waiting 3s for consumer group rebalance...");
        Thread.sleep(3000);

        log("Publishing " + messageCount + " messages...");
        for (int i = 0; i < messageCount; i++) {
            client.pub(topic, ("message-" + i).getBytes());
            log("Published message-" + i);
        }

        log("Waiting up to 15s for all messages...");
        boolean allReceived = latch.await(15, TimeUnit.SECONDS);
        log("Latch result: " + allReceived + " (received " + received.size() + "/" + messageCount + ")");

        assertTrue(allReceived, "Not all messages received within timeout");
        assertEquals(messageCount, received.size());
        log("Test passed");
    }

    @Test
    @Order(3)
    void pub_sub_multipleTopics() throws InterruptedException {
        String topic0 = CommitTopics.forPartition(0);
        String topic1 = CommitTopics.forPartition(1);

        log("Subscribing to topics: " + topic0 + ", " + topic1);
        CountDownLatch latch = new CountDownLatch(2);
        CopyOnWriteArrayList<String> receivedTopics = new CopyOnWriteArrayList<>();

        client.sub(topic0, (t, offset, message) -> {
            log("Message on " + t + " offset=" + offset);
            receivedTopics.add(t);
            latch.countDown();
        });
        client.sub(topic1, (t, offset, message) -> {
            log("Message on " + t + " offset=" + offset);
            receivedTopics.add(t);
            latch.countDown();
        });

        log("Waiting 3s for consumer group rebalance...");
        Thread.sleep(3000);

        log("Publishing to both topics...");
        client.pub(topic0, "msg0".getBytes());
        client.pub(topic1, "msg1".getBytes());

        log("Waiting up to 15s for both messages...");
        boolean allReceived = latch.await(15, TimeUnit.SECONDS);
        log("Latch result: " + allReceived + " topics received: " + receivedTopics);

        assertTrue(allReceived, "Messages not received within timeout");
        assertTrue(receivedTopics.contains(topic0));
        assertTrue(receivedTopics.contains(topic1));
        log("Test passed");
    }

    @Test
    @Order(4)
    void drop_stopsDelivery() throws InterruptedException {
        String topic = CommitTopics.forPartition(2);

        log("Subscribing to topic: " + topic);
        CopyOnWriteArrayList<byte[]> received = new CopyOnWriteArrayList<>();
        CountDownLatch firstLatch = new CountDownLatch(1);
        client.sub(topic, (t, offset, message) -> {
            log("Message received: " + new String(message));
            received.add(message);
            firstLatch.countDown();
        });

        log("Waiting 3s for consumer group rebalance...");
        Thread.sleep(3000);

        log("Publishing before-drop message...");
        client.pub(topic, "before-drop".getBytes());

        log("Waiting up to 10s for before-drop message...");
        boolean gotFirst = firstLatch.await(10, TimeUnit.SECONDS);
        log("before-drop received: " + gotFirst);

        log("Dropping subscription...");
        client.drop(topic);
        Thread.sleep(500);

        int countAfterDrop = received.size();
        log("Count after drop: " + countAfterDrop + " — publishing after-drop message");
        client.pub(topic, "after-drop".getBytes());

        log("Waiting 3s to confirm after-drop message is NOT received...");
        Thread.sleep(3000);

        log("Final count: " + received.size() + " (expected: " + countAfterDrop + ")");
        assertEquals(countAfterDrop, received.size(),
                "No new messages should arrive after drop");
        log("Test passed");
    }

    @Test
    @Order(5)
    void uniqueGroupIds_allReceiveMessage() throws Exception {
        String topic = CommitTopics.forPartition(3);
        byte[] payload = "broadcast".getBytes();

        log("Creating second KafkaPubSub client with unique group ID...");
        String groupId2 = "test-group-b-" + System.currentTimeMillis();
        KafkaPubSub pubSub2 = new KafkaPubSub(kafka.getBootstrapServers(), groupId2);
        PubSubClient client2 = new PubSubClient(pubSub2);
        client2.start();
        log("Second client started with groupId=" + groupId2);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        log("Subscribing both clients to topic: " + topic);
        client.sub(topic, (t, offset, message) -> {
            log("Client 1 received message on " + t);
            latch1.countDown();
        });
        client2.sub(topic, (t, offset, message) -> {
            log("Client 2 received message on " + t);
            latch2.countDown();
        });

        log("Waiting 3s for both consumer groups to rebalance...");
        Thread.sleep(3000);

        log("Publishing broadcast message...");
        client.pub(topic, payload);

        try {
            log("Waiting up to 15s for client 1...");
            boolean got1 = latch1.await(15, TimeUnit.SECONDS);
            log("Client 1 received: " + got1);

            log("Waiting up to 15s for client 2...");
            boolean got2 = latch2.await(15, TimeUnit.SECONDS);
            log("Client 2 received: " + got2);

            assertTrue(got1, "Client 1 did not receive message");
            assertTrue(got2, "Client 2 did not receive message");
            log("Test passed");
        } finally {
            log("Closing second client...");
            client2.close();
        }
    }
}