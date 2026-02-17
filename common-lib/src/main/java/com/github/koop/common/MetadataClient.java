package com.github.koop.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetadataClient implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(MetadataClient.class.getName());
    private final Client client;
    private final KV kvClient;
    private final Watch watchClient;
    private final ObjectMapper objectMapper;
    private final ByteSequence replicaSetKey;
    private final ByteSequence partitionSpreadKey;

    private volatile ReplicaSetConfiguration replicaSetConfiguration;
    private volatile PartitionSpreadConfiguration partitionSpreadConfiguration;

    private Watch.Watcher replicaSetWatcher;
    private Watch.Watcher partitionSpreadWatcher;

    private final Set<BiConsumer<ReplicaSetConfiguration, ReplicaSetConfiguration>> replicaSetUpdateListeners;
    private final Set<BiConsumer<PartitionSpreadConfiguration, PartitionSpreadConfiguration>> partitionSpreadUpdateListeners;

    public MetadataClient(String etcdUrl, String replicaSetKey, String partitionSpreadKey) {
        if (etcdUrl == null || etcdUrl.isEmpty()) {
            throw new IllegalStateException("ETCD_URL environment variable is not set");
        }
        this.client = Client.builder().endpoints(etcdUrl).build();
        this.kvClient = client.getKVClient();
        this.watchClient = client.getWatchClient();
        this.objectMapper = new ObjectMapper();
        this.replicaSetKey = ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8);
        this.partitionSpreadKey = ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8);
        this.replicaSetUpdateListeners = new CopyOnWriteArraySet<>();
        this.partitionSpreadUpdateListeners = new CopyOnWriteArraySet<>();
    }

    public MetadataClient(String replicaSetKey, String partitionSpreadKey) {
        this(System.getenv("ETCD_URL"), replicaSetKey, partitionSpreadKey);
    }

    public MetadataClient(Client client, String replicaSetKey, String partitionSpreadKey) {
        this.client = client;
        this.kvClient = client.getKVClient();
        this.watchClient = client.getWatchClient();
        this.objectMapper = new ObjectMapper();
        this.replicaSetKey = ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8);
        this.partitionSpreadKey = ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8);
        this.replicaSetUpdateListeners = new CopyOnWriteArraySet<>();
        this.partitionSpreadUpdateListeners = new CopyOnWriteArraySet<>();
    }

    public void addReplicaSetUpdateListener(BiConsumer<ReplicaSetConfiguration, ReplicaSetConfiguration> listener) {
        this.replicaSetUpdateListeners.add(listener);
    }

    public void addPartitionSpreadUpdateListener(
            BiConsumer<PartitionSpreadConfiguration, PartitionSpreadConfiguration> listener) {
        this.partitionSpreadUpdateListeners.add(listener);
    }

    public void start() {
        try {
            // 1. Initial Synchronous Fetch
            updateReplicaSetConfigurationFromStore();
            updatePartitionSpreadConfigurationFromStore();

            // 2. Setup Asynchronous Watch for ReplicaSetConfiguration
            this.replicaSetWatcher = watchClient.watch(replicaSetKey, new Watch.Listener() {
                @Override
                public void onNext(WatchResponse response) {
                    for (WatchEvent event : response.getEvents()) {
                        var prev = replicaSetConfiguration;
                        if(event.getEventType() == WatchEvent.EventType.PUT) {
                            LOGGER.info("ReplicaSetConfiguration update detected in etcd.");
                            parseAndSetReplicaSetConfiguration(event.getKeyValue().getValue());
                        }else if(event.getEventType() == WatchEvent.EventType.DELETE) {
                            LOGGER.warning("ReplicaSetConfiguration key deleted from etcd.");
                            replicaSetConfiguration = null;
                        }
                        var newConfig = replicaSetConfiguration;
                        for (var listener : replicaSetUpdateListeners) {
                            try {
                                listener.accept(prev, newConfig);
                            } catch (Exception e) {
                                LOGGER.log(Level.SEVERE, "Error in replicaSet update listener", e);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    LOGGER.log(Level.SEVERE, "Error watching replicaSetConfiguration", throwable);
                }

                @Override
                public void onCompleted() {
                    LOGGER.info("ReplicaSetConfiguration watch completed.");
                }
            });

            // 3. Setup Asynchronous Watch for PartitionSpreadConfiguration
            this.partitionSpreadWatcher = watchClient.watch(partitionSpreadKey, new Watch.Listener() {
                @Override
                public void onNext(WatchResponse response) {
                    for (WatchEvent event : response.getEvents()) {
                        var prev = partitionSpreadConfiguration;
                        if(event.getEventType() == WatchEvent.EventType.PUT) {
                            LOGGER.info("PartitionSpreadConfiguration update detected in etcd.");
                            parseAndSetPartitionSpreadConfiguration(event.getKeyValue().getValue());
                        }else if(event.getEventType() == WatchEvent.EventType.DELETE) {
                            LOGGER.warning("PartitionSpreadConfiguration key deleted from etcd.");
                            partitionSpreadConfiguration = null;
                        }
                        var newConfig = partitionSpreadConfiguration;
                        for (var listener : partitionSpreadUpdateListeners) {
                            try {
                                listener.accept(prev, newConfig);
                            } catch (Exception e) {
                                LOGGER.log(Level.SEVERE, "Error in partition_spread update listener", e);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    LOGGER.log(Level.SEVERE, "Error watching partitionSpreadConfiguration", throwable);
                }

                @Override
                public void onCompleted() {
                    LOGGER.info("PartitionSpreadConfiguration watch completed.");
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to start metadata client", e);
        }
    }

    public void registerReplicaSetUpdateListener(Watch.Listener listener) {
        if (replicaSetWatcher != null) {
            replicaSetWatcher.close();
        }
        this.replicaSetWatcher = watchClient.watch(replicaSetKey, listener);
    }

    private void updateReplicaSetConfigurationFromStore() {
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(replicaSetKey);
            GetResponse response = getFuture.get();
            if (response.getKvs().isEmpty()) {
                LOGGER.warning(
                        "No replicaSetConfiguration found for key: " + replicaSetKey.toString(StandardCharsets.UTF_8));
                return;
            }
            parseAndSetReplicaSetConfiguration(response.getKvs().get(0).getValue());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to fetch initial replicaSetConfiguration", e);
        }
    }

    private void updatePartitionSpreadConfigurationFromStore() {
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(partitionSpreadKey);
            GetResponse response = getFuture.get();
            if (response.getKvs().isEmpty()) {
                LOGGER.warning("No partitionSpreadConfiguration found for key: "
                        + partitionSpreadKey.toString(StandardCharsets.UTF_8));
                return;
            }
            parseAndSetPartitionSpreadConfiguration(response.getKvs().get(0).getValue());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to fetch initial partitionSpreadConfiguration", e);
        }
    }

    private void parseAndSetReplicaSetConfiguration(ByteSequence bs) {
        try {
            String json = bs.toString(StandardCharsets.UTF_8);
            this.replicaSetConfiguration = objectMapper.readValue(json, ReplicaSetConfiguration.class);
            LOGGER.info("ReplicaSetConfiguration updated successfully.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse replicaSetConfiguration JSON", e);
        }
    }

    private void parseAndSetPartitionSpreadConfiguration(ByteSequence bs) {
        try {
            String json = bs.toString(StandardCharsets.UTF_8);
            this.partitionSpreadConfiguration = objectMapper.readValue(json, PartitionSpreadConfiguration.class);
            LOGGER.info("PartitionSpreadConfiguration updated successfully.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse partitionSpreadConfiguration JSON", e);
        }
    }

    public ReplicaSetConfiguration getReplicaSetConfiguration() {
        return replicaSetConfiguration;
    }

    public PartitionSpreadConfiguration getPartitionSpreadConfiguration() {
        return partitionSpreadConfiguration;
    }

    @Override
    public void close() {
        if (replicaSetWatcher != null) {
            replicaSetWatcher.close();
        }
        if (partitionSpreadWatcher != null) {
            partitionSpreadWatcher.close();
        }
        if (client != null) {
            client.close();
        }
    }
}