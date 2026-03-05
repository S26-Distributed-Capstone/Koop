package com.github.koop.storagenode.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryStorageStrategy implements StorageStrategy {
    // SkipListMap keeps keys sorted, just like RocksDB
    private final ConcurrentSkipListMap<Long, OpLog> opLogTable = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, Metadata> metadataTable = new ConcurrentHashMap<>();

    @Override
    public void addLog(long sequenceNumber, OpLog log) {
        opLogTable.put(sequenceNumber, log);
    }

    @Override
    public void updateMetadata(String fileKey, Metadata metadata) {
        metadataTable.put(fileKey, metadata);
    }

    @Override
    public Metadata getMetadata(String fileKey) {
        return metadataTable.get(fileKey);
    }

    @Override
    public void close() {
        opLogTable.clear();
        metadataTable.clear();
    }
}
