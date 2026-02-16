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
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetadataClient implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(MetadataClient.class.getName());
    private final Client client;
    private final KV kvClient;
    private final Watch watchClient;
    private final ObjectMapper objectMapper;
    private final ByteSequence key;
    
    private volatile Metadata metadata;
    private Watch.Watcher watcher;

    public MetadataClient(String metadataKey) {
        String etcdUrl = System.getenv("ETCD_URL");
        if (etcdUrl == null || etcdUrl.isEmpty()) {
            throw new IllegalStateException("ETCD_URL environment variable is not set");
        }
        this.client = Client.builder().endpoints(etcdUrl).build();
        this.kvClient = client.getKVClient();
        this.watchClient = client.getWatchClient();
        this.objectMapper = new ObjectMapper();
        this.key = ByteSequence.from(metadataKey, StandardCharsets.UTF_8);
    }

    public MetadataClient(Client client, String metadataKey) {
        this.client = client;
        this.kvClient = client.getKVClient();
        this.watchClient = client.getWatchClient();
        this.objectMapper = new ObjectMapper();
        this.key = ByteSequence.from(metadataKey, StandardCharsets.UTF_8);
    }

    public void start() {
        try {
            // 1. Initial Synchronous Fetch
            updateMetadataFromStore();

            // 2. Setup Asynchronous Watch
            this.watcher = watchClient.watch(key, new Watch.Listener() {
                @Override
                public void onNext(WatchResponse response) {
                    for (WatchEvent event : response.getEvents()) {
                        if (event.getEventType() == WatchEvent.EventType.PUT) {
                            LOGGER.info("Metadata update detected in etcd.");
                            parseAndSetMetadata(event.getKeyValue().getValue());
                        } else if (event.getEventType() == WatchEvent.EventType.DELETE) {
                            LOGGER.warning("Metadata key deleted from etcd.");
                            metadata = null;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    LOGGER.log(Level.SEVERE, "Error watching metadata", throwable);
                }

                @Override
                public void onCompleted() {
                    LOGGER.info("Metadata watch completed.");
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to start metadata client", e);
        }
    }

    private void updateMetadataFromStore() {
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(key);
            GetResponse response = getFuture.get();
            if (response.getKvs().isEmpty()) {
                LOGGER.warning("No metadata found for key: " + key.toString(StandardCharsets.UTF_8));
                return;
            }
            parseAndSetMetadata(response.getKvs().get(0).getValue());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to fetch initial metadata", e);
        }
    }

    private void parseAndSetMetadata(ByteSequence bs) {
        try {
            String json = bs.toString(StandardCharsets.UTF_8);
            this.metadata = objectMapper.readValue(json, Metadata.class);
            LOGGER.info("Metadata updated successfully.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse metadata JSON", e);
        }
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public void close() {
        if (watcher != null) {
            watcher.close();
        }
        if (client != null) {
            client.close();
        }
    }
}