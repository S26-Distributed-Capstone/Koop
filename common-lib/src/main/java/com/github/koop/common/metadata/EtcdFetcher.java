package com.github.koop.common.metadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

public class EtcdFetcher implements Fetcher {

    private final Map<Class<? extends Object>, ByteSequence> metadataKeyMap;
    private final Map<Class<? extends Object>, Object> currentMetadataCache;
    private final Client etcdClient;
    private final ObjectMapper objectMapper;
    private final Logger logger = LogManager.getLogger(EtcdFetcher.class);
    private ChangeListener<Object> listener;

    private EtcdFetcher(String etcdEndpoint, Map<Class<? extends Object>, String> metadataKeyMap) {
        this.metadataKeyMap = metadataKeyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> ByteSequence.from(e.getValue(), StandardCharsets.UTF_8)));
        this.currentMetadataCache = new ConcurrentHashMap<>();
        this.etcdClient = Client.builder().endpoints(etcdEndpoint).build();
        this.objectMapper = new ObjectMapper();
    }

    public static EtcdFetcher start(String etcdEndpoint, Map<Class<? extends Object>, String> metadataKeyMap,
            ChangeListener<Object> onChange) {
        EtcdFetcher fetcher = new EtcdFetcher(etcdEndpoint, metadataKeyMap);
        fetcher.start(onChange);
        return fetcher;
    }

    @Override
    public void start(ChangeListener<Object> onChange) {
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
                        Object prev = currentMetadataCache.get(clazz);
                        if (event.getEventType() == io.etcd.jetcd.watch.WatchEvent.EventType.PUT) {
                            try {
                                String valueStr = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                Object valueObj = objectMapper.readValue(valueStr, clazz);
                                currentMetadataCache.put(clazz, valueObj);
                                if (listener != null) {
                                    listener.onChange(prev, valueObj);
                                }
                            } catch (JsonProcessingException e) {
                                logger.error("Failed to parse metadata for key: " + key.toString(), e);
                            }
                        } else if (event.getEventType() == io.etcd.jetcd.watch.WatchEvent.EventType.DELETE) {
                            currentMetadataCache.remove(clazz);
                            if (listener != null) {
                                listener.onChange(prev, null);
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

                Object prev = currentMetadataCache.put(clazz, valueObj);
                if (listener != null && prev != valueObj) {
                    listener.onChange(prev, valueObj);
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
    public <T> T fetchCurrent(Class<T> clazz) {
        return clazz.cast(currentMetadataCache.get(clazz));
    }

    @Override
    public void close() {
        if (this.etcdClient != null) {
            this.etcdClient.close();
        }
    }

}
