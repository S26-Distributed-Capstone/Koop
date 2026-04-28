package com.github.koop.common.metadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

public class EtcdFetcher implements Fetcher {

    private final Map<Class<? extends Object>, ByteSequence> metadataKeyMap;
    private final Client etcdClient;
    private final ObjectMapper objectMapper;
    private final Logger logger = LogManager.getLogger(EtcdFetcher.class);
    private Consumer<Object> listener;

    public EtcdFetcher(String etcdEndpoint, Map<Class<? extends Object>, String> metadataKeyMap) {
        if (etcdEndpoint == null || etcdEndpoint.isEmpty()) {
            throw new IllegalArgumentException("ETCD endpoint must be provided");
        }
        System.out.println(">>> EtcdFetcher endpoint: [" + etcdEndpoint + "]");
        this.metadataKeyMap = metadataKeyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> ByteSequence.from(e.getValue(), StandardCharsets.UTF_8)));
        this.etcdClient = Client.builder().endpoints(etcdEndpoint).build();
        this.objectMapper = new ObjectMapper();
    }

    public EtcdFetcher(Map<Class<? extends Object>, String> metadataKeyMap) {
        this(System.getenv("ETCD_URL"), metadataKeyMap);
    }

    @Override
    public void start(Consumer<Object> onChange) {
        this.listener = onChange;
        fetchInitialMetadata();
        setupWatchers();
    }

    private final java.util.List<io.etcd.jetcd.Watch.Watcher> watchers = new java.util.ArrayList<>();

    private void setupWatchers() {
        io.etcd.jetcd.Watch watchClient = etcdClient.getWatchClient();
        metadataKeyMap.forEach((clazz, key) -> {
            watchers.add(watchClient.watch(key, new io.etcd.jetcd.Watch.Listener() {
                @Override
                public void onNext(io.etcd.jetcd.watch.WatchResponse response) {
                    for (io.etcd.jetcd.watch.WatchEvent event : response.getEvents()) {
                        if (event.getEventType() == io.etcd.jetcd.watch.WatchEvent.EventType.PUT) {
                            try {
                                String valueStr = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                Object valueObj = objectMapper.readValue(valueStr, clazz);
                                if (listener != null) {
                                    listener.accept(valueObj);
                                }
                            } catch (JsonProcessingException e) {
                                logger.error("Failed to parse metadata for key: " + key.toString(), e);
                            }
                        } else if (event.getEventType() == io.etcd.jetcd.watch.WatchEvent.EventType.DELETE) {
                            if (listener != null) {
                                listener.accept(null);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("Error watching key: " + key.toString(), throwable);
                }

                @Override
                public void onCompleted() {
                    logger.info("Watch completed for key: " + key.toString());
                }
            }));
        });
    }

    private void fetchInitialMetadata() {
        metadataKeyMap.keySet().forEach(this::fetchMetadata);
    }

    private <T> void fetchMetadata(Class<T> clazz) {
        ByteSequence key = metadataKeyMap.get(clazz);
        try {
            var kvs = etcdClient.getKVClient().get(key).get().getKvs();
            if (!kvs.isEmpty()) {
                var valueStr = kvs.get(0).getValue().toString(StandardCharsets.UTF_8);
                Object valueObj = objectMapper.readValue(valueStr, clazz);

                if (listener != null) {
                    listener.accept(valueObj);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Failed to fetch metadata for key: " + key.toString(), e);
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse metadata for key: " + key.toString(), e);
        }
    }

    @Override
    public void close() {
        if (this.etcdClient != null) {
            this.etcdClient.close();
        }
    }

}
