package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryStorageStrategy implements StorageStrategy {
    // Table #1 — operation log (grouped by partition, sorted by seqNum)
    private final ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, OpLog>> opLogsByPartition = new ConcurrentHashMap<>();
    // Table #2 — metadata (sorted by key for prefix scans)
    private final ConcurrentSkipListMap<String, Metadata> metadataTable = new ConcurrentSkipListMap<>();
    // Table #3 — buckets (sorted by key for streaming)
    private final ConcurrentSkipListMap<String, Bucket> bucketsTable = new ConcurrentSkipListMap<>();

    // --- Table #1: Operation Log ---

    @Override
    public void addLog(OpLog log) {
        opLogsByPartition
                .computeIfAbsent(log.partition(), p -> new ConcurrentSkipListMap<>())
                .put(log.seqNum(), log);
    }

    @Override
    public Optional<OpLog> getLog(int partition, long seqNum) {
        ConcurrentSkipListMap<Long, OpLog> partitionLogs = opLogsByPartition.get(partition);
        return partitionLogs == null ? Optional.empty() : Optional.ofNullable(partitionLogs.get(seqNum));
    }

    @Override
    public Stream<OpLog> getLogs(int partition, long from, long downTo) {
        ConcurrentSkipListMap<Long, OpLog> partitionLogs = opLogsByPartition.get(partition);
        if (partitionLogs == null) return Stream.empty();
        return partitionLogs.subMap(downTo, true, from, true).reversed().values().stream();
    }

    // --- Table #2: Metadata ---

    @Override
    public void updateMetadata(Metadata metadata) {
        metadataTable.put(metadata.key(), metadata);
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) {
        addLog(log);
        updateMetadata(metadata);
    }

    @Override
    public Optional<Metadata> getMetadata(String key) {
        return Optional.ofNullable(metadataTable.get(key));
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) {
        return metadataTable.tailMap(prefix, true).values().stream()
                .takeWhile(m -> m.key().startsWith(prefix));
    }

    // --- Table #3: Buckets ---

    @Override
    public void updateBucket(Bucket bucket) {
        bucketsTable.put(bucket.key(), bucket);
    }

    @Override
    public void atomicallyUpdateLogAndBucket(OpLog log, Bucket bucket) {
        addLog(log);
        updateBucket(bucket);
    }

    @Override
    public Optional<Bucket> getBucket(String key) {
        return Optional.ofNullable(bucketsTable.get(key));
    }

    @Override
    public Stream<Bucket> streamBuckets() {
        return bucketsTable.values().stream();
    }

    // --- Lifecycle ---

    @Override
    public void close() {
        opLogsByPartition.clear();
        metadataTable.clear();
        bucketsTable.clear();
    }
}