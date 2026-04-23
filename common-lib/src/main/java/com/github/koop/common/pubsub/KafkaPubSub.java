package com.github.koop.common.pubsub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka-backed implementation of {@link PubSub}.
 *
 * <p>Each instance creates one {@link KafkaProducer} for publishing and one
 * {@link KafkaConsumer} running on a dedicated virtual thread for consuming.
 * The consumer polls in a tight loop and dispatches messages to the single
 * {@link PubSubListener} registered via {@link #start}.
 *
 * <h2>Consumer group design</h2>
 * Each SN instance gets a <em>unique</em> consumer group ID so that every SN
 * receives every commit message published to the partitions it owns. If SNs
 * shared a group, Kafka would load-balance partitions across them and each SN
 * would only see a subset of commits — causing silent data loss.
 *
 * <p>The group ID defaults to {@code koop-{hostname}-{pid}} but can be
 * overridden via the {@code KAFKA_CONSUMER_GROUP_ID} environment variable.
 *
 * <h2>Configuration</h2>
 * Reads {@code KAFKA_BOOTSTRAP_SERVERS} from the environment
 * (e.g. {@code kafka:9092}). Throws {@link IllegalStateException} at
 * construction time if the variable is not set.
 *
 * <h2>Topic management</h2>
 * Topics follow the {@code partition-N} naming convention defined by
 * {@link CommitTopics}. Topics must exist before messages are published or
 * consumed — use {@code auto.create.topics.enable=true} on the broker for
 * development, or pre-create them in production.
 */
public class KafkaPubSub implements PubSub {

    private static final Logger logger = LogManager.getLogger(KafkaPubSub.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final long CLOSE_TIMEOUT_SECONDS = 10;

    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean subscriptionDirty = new AtomicBoolean(false);
    private final BlockingQueue<Runnable> subscriptionChanges = new LinkedBlockingQueue<>();
    private final ExecutorService consumerThread =
    Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("kafka-consumer-", 0).factory());

    private volatile PubSubListener listener;

    // ─── Construction ─────────────────────────────────────────────────────────

    /**
     * Creates a {@code KafkaPubSub} using the {@code KAFKA_BOOTSTRAP_SERVERS}
     * environment variable.
     */
    public KafkaPubSub() {
        this(requireBootstrapServers(), resolveGroupId());
    }

    /**
     * Creates a {@code KafkaPubSub} with explicit bootstrap servers and group ID.
     * Prefer the no-arg constructor in production; this overload exists for tests.
     */
    public KafkaPubSub(String bootstrapServers, String groupId) {
        this.producer = new KafkaProducer<>(producerProperties(bootstrapServers));
        this.consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers, groupId));
        logger.info("KafkaPubSub created — brokers={} groupId={}", bootstrapServers, groupId);
    }

    // ─── PubSub interface ─────────────────────────────────────────────────────

    /**
     * Publishes {@code message} to {@code topic} asynchronously.
     * Errors are logged but not rethrown — consistent with fire-and-forget
     * publish semantics used throughout the commit protocol.
     */
    @Override
    public void pub(String topic, byte[] message) {
        producer.send(new ProducerRecord<>(topic, message), (metadata, ex) -> {
            if (ex != null) {
                logger.error("Failed to publish to topic {}: {}", topic, ex.getMessage(), ex);
            } else {
                logger.trace("Published to topic {} partition {} offset {}",
                        topic, metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Subscribes to {@code topic}. The subscription change is queued and applied
     * on the consumer poll thread to avoid KafkaConsumer thread-safety violations.
     */
    @Override
    public void sub(String topic) {
        subscribedTopics.add(topic);
        subscriptionChanges.offer(() -> updateConsumerSubscription());
        consumer.wakeup(); // interrupt current poll() so the change is picked up quickly
        logger.debug("Queued subscription for topic {}", topic);
    }

    /**
     * Unsubscribes from {@code topic}. The change is queued and applied on the
     * consumer poll thread.
     */
    @Override
    public void drop(String topic) {
        subscribedTopics.remove(topic);
        subscriptionChanges.offer(() -> updateConsumerSubscription());
        consumer.wakeup();
        logger.debug("Queued drop for topic {}", topic);
    }

    /**
     * Starts the consumer poll loop on a dedicated daemon thread.
     * Must be called once before messages will be delivered to {@code listener}.
     *
     * @param listener the single listener that receives all consumed messages.
     */
    @Override
    public void start(PubSubListener listener) {
        this.listener = listener;
        running.set(true);
        consumerThread.submit(this::pollLoop);
        logger.info("KafkaPubSub consumer loop started");
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup(); // interrupts the blocking poll() call so loop checks running flag
        consumerThread.shutdown();
        try {
            if (!consumerThread.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warn("Kafka consumer thread did not stop within {}s", CLOSE_TIMEOUT_SECONDS);
                consumerThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            consumerThread.shutdownNow();
        }
        try {
            producer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            logger.warn("Error closing Kafka producer: {}", e.getMessage());
        }
        try {
            consumer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            logger.warn("Error closing Kafka consumer: {}", e.getMessage());
        }
        logger.info("KafkaPubSub closed");
    }

    // ─── Private helpers ──────────────────────────────────────────────────────

    private void pollLoop() {
        try {
            while (running.get()) {
                // Drain any pending subscription changes on this thread
                // (KafkaConsumer is not thread-safe — subscribe() must be called here)
                Runnable change;
                while ((change = subscriptionChanges.poll()) != null) {
                    change.run();
                }

                // If not subscribed to anything, skip poll to avoid IllegalStateException
                if (subscribedTopics.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }

                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    records.forEach(record -> {
                        PubSubListener l = listener;
                        if (l != null) {
                            try {
                                l.onMessage(record.topic(), record.offset(), record.value());
                            } catch (Exception e) {
                                logger.error("Error dispatching message from topic {} offset {}: {}",
                                        record.topic(), record.offset(), e.getMessage(), e);
                            }
                        }
                    });
                } catch (WakeupException e) {
                    // Wakeup was triggered by sub()/drop()/close() — loop back to
                    // drain the subscription queue or check running flag
                    logger.debug("Consumer woken up — reprocessing subscription changes");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Kafka consumer poll loop failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Re-subscribes the consumer to the current set of topics.
     * MUST be called from the consumer poll thread only.
     */
    private void updateConsumerSubscription() {
        if (subscribedTopics.isEmpty()) {
            consumer.unsubscribe();
            logger.debug("Unsubscribed from all topics");
        } else {
            consumer.subscribe(new ArrayList<>(subscribedTopics));
            logger.debug("Updated subscription to topics: {}", subscribedTopics);
        }
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Wait for all in-sync replicas to acknowledge — prevents data loss
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Retry transient broker errors
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        return props;
    }

    private static Properties consumerProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Start from the earliest offset when a new group first connects
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Commit offsets automatically — commit messages are idempotent so
        // at-least-once delivery is acceptable
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        return props;
    }

    private static String requireBootstrapServers() {
        String brokers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (brokers == null || brokers.isBlank()) {
            throw new IllegalStateException(
                    "KAFKA_BOOTSTRAP_SERVERS environment variable is not set");
        }
        return brokers;
    }

    /**
     * Builds a unique consumer group ID from hostname and PID so that every
     * SN instance receives all messages for its partitions independently.
     * Can be overridden via {@code KAFKA_CONSUMER_GROUP_ID}.
     */
    private static String resolveGroupId() {
        String override = System.getenv("KAFKA_CONSUMER_GROUP_ID");
        if (override != null && !override.isBlank()) {
            return override;
        }
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            long pid = ProcessHandle.current().pid();
            return "koop-" + hostname + "-" + pid;
        } catch (Exception e) {
            return "koop-" + System.currentTimeMillis();
        }
    }
}