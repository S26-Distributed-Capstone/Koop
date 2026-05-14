package com.github.koop.common.pubsub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link KafkaPubSub}.
 *
 * Run with: mvn test -pl common-lib -Dtest=KafkaPubSubTest
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class KafkaPubSubTest {

    private static final Logger log = LogManager.getLogger(KafkaPubSubTest.class);

    private ConfluentKafkaContainer kafka;

    @BeforeAll
    void startKafka() {
        kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.8");
        kafka.start();
        log.info("Kafka container started, bootstrap={}", kafka.getBootstrapServers());
    }

    @AfterAll
    void stopKafka() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @Disabled
    void publish_thenSubscribe_deliversMessage() {
        String topic = CommitTopics.forPartition(0);
        String groupId = "test-group-" + System.currentTimeMillis();

        KafkaPubSub pubSub = new KafkaPubSub(kafka.getBootstrapServers(), groupId);
        PubSubClient client = new PubSubClient(pubSub);
        client.start();

        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<byte[]> received = new CopyOnWriteArrayList<>();

        client.sub(topic, (t, offset, message) -> {
            received.add(message);
            latch.countDown();
        });

        // Publish repeatedly until consumer group assignment completes and the
        // first message lands. Avoids fragile fixed sleeps for broker readiness.
        try {
            await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    client.pub(topic, "hello".getBytes());
                    return latch.getCount() == 0;
                });

            assertEquals(1, received.size(), "expected exactly one delivery");
            assertArrayEquals("hello".getBytes(), received.get(0));
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("error closing client", e);
            }
        }
    }
}
