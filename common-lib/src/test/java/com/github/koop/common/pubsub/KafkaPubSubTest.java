package com.github.koop.common.pubsub;

import org.junit.jupiter.api.*;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.LocalTime;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link KafkaPubSub}.
 *
 * Run with:
 *   mvn test -pl common-lib -Dtest=KafkaPubSubTest
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaPubSubTest {

    // Manually managed so we can log every lifecycle step
    static ConfluentKafkaContainer kafka;

    private static void log(String msg) {
        System.out.printf("[%s] %s%n", LocalTime.now().withNano(0), msg);
        System.out.flush();
    }

    @BeforeAll
    void startKafka() {
        log("Creating ConfluentKafkaContainer...");
        kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.8");
        log("Starting container...");
        kafka.start();
        log("Container started. Bootstrap: " + kafka.getBootstrapServers());
    }

    @AfterAll
    void stopKafka() {
        log("Stopping Kafka container...");
        if (kafka != null) kafka.stop();
        log("Done.");
    }

    @Test
    @Order(1)
    void pub_sub_roundTrip() throws InterruptedException {
        String topic = CommitTopics.forPartition(0);
        String groupId = "test-group-" + System.currentTimeMillis();

        log("Creating KafkaPubSub groupId=" + groupId);
        KafkaPubSub pubSub = new KafkaPubSub(kafka.getBootstrapServers(), groupId);
        PubSubClient client = new PubSubClient(pubSub);

        log("Starting client...");
        client.start();

        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<byte[]> received = new CopyOnWriteArrayList<>();

        log("Subscribing to topic: " + topic);
        client.sub(topic, (t, offset, message) -> {
            log("*** MESSAGE RECEIVED on " + t + " offset=" + offset + " value=" + new String(message));
            received.add(message);
            latch.countDown();
        });

        log("Waiting 5s for consumer group assignment...");
        Thread.sleep(5000);

        log("Publishing message...");
        client.pub(topic, "hello".getBytes());
        log("Message published. Waiting 15s for delivery...");

        boolean ok = latch.await(15, TimeUnit.SECONDS);
        log("Latch done: " + ok + " received=" + received.size());

        try {
            client.close();
        } catch (Exception e) {
            log("Error closing client: " + e.getMessage());
        }

        assertTrue(ok, "Message not received within timeout");
        assertArrayEquals("hello".getBytes(), received.get(0));
        log("Test passed.");
    }
}