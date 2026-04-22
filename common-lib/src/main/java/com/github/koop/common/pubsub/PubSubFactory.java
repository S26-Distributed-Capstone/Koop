package com.github.koop.common.pubsub;

/**
 * Factory for creating the appropriate {@link PubSub} implementation based
 * on the runtime environment.
 *
 * <p>If {@code KAFKA_BOOTSTRAP_SERVERS} is set, returns a {@link KafkaPubSub}.
 * Otherwise falls back to {@link MemoryPubSub}, which is suitable for
 * single-process development and all unit/integration tests.
 *
 * <p>Usage:
 * <pre>
 *     PubSubClient pubSubClient = new PubSubClient(PubSubFactory.create());
 *     pubSubClient.start();
 * </pre>
 */
public final class PubSubFactory {

    private PubSubFactory() {}

    /**
     * Returns a {@link KafkaPubSub} if {@code KAFKA_BOOTSTRAP_SERVERS} is set,
     * otherwise a {@link MemoryPubSub}.
     */
    public static PubSub create() {
        String brokers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (brokers != null && !brokers.isBlank()) {
            return new KafkaPubSub();
        }
        return new MemoryPubSub();
    }
}