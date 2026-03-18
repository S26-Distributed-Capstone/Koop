package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryStorageStrategy implements StorageStrategy {
    // Table #1 — operation log (sorted by seqNum for range scans)
    private final ConcurrentSkipListMap<Long, OpLog> opLogTable = new ConcurrentSkipListMap<>();
    // Table #2 — metadata (sorted by key for prefix scans)
    private final ConcurrentSkipListMap<String, Metadata> metadataTable = new ConcurrentSkipListMap<>();
    // Table #3 — buckets (sorted by key for streaming)
    private final ConcurrentSkipListMap<String, Bucket> bucketsTable = new ConcurrentSkipListMap<>();
    // Table #4 — multipart uploads (unordered, keyed by object key)
    private final ConcurrentHashMap<String, MultipartUpload> multipartTable = new ConcurrentHashMap<>();

    // --- Table #1: Operation Log ---

    @Override
    public void addLog(OpLog log) {
        opLogTable.put(log.seqNum(), log);
    }

    @Override
    public Optional<OpLog> getLog(long seqNum) {
        return Optional.ofNullable(opLogTable.get(seqNum));
    }

    @Override
    public Stream<OpLog> getLogs(long from, long downTo) {
        return opLogTable.subMap(downTo, true, from, true).reversed().values().stream();
    }

    // --- Table #2: Metadata ---

    @Override
    public void updateMetadata(Metadata metadata) {
        metadataTable.put(metadata.fileName(), metadata);
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) {
        addLog(log);
        updateMetadata(metadata);
    }

    @Override
    public Optional<Metadata> getMetadata(String fileKey) {
        return Optional.ofNullable(metadataTable.get(fileKey));
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) {
        return metadataTable.tailMap(prefix, true).values().stream()
                .takeWhile(metadata -> metadata.fileName().startsWith(prefix));
    }

    // --- Table #3: Buckets ---

    @Override
    public void putBucket(Bucket bucket) {
        bucketsTable.put(bucket.key(), bucket);
    }

    @Override
    public Optional<Bucket> getBucket(String key) {
        return Optional.ofNullable(bucketsTable.get(key));
    }

    @Override
    public void deleteBucket(String key) {
        bucketsTable.remove(key);
    }

    @Override
    public Stream<Bucket> streamBuckets() {
        return bucketsTable.values().stream();
    }

    // --- Table #4: Multipart Uploads ---

    @Override
    public void putMultipartUpload(MultipartUpload upload) {
        multipartTable.put(upload.key(), upload);
    }

    @Override
    public Optional<MultipartUpload> getMultipartUpload(String key) {
        return Optional.ofNullable(multipartTable.get(key));
    }

    @Override
    public void deleteMultipartUpload(String key) {
        multipartTable.remove(key);
    }

    // --- Lifecycle ---

    @Override
    public void close() {
        opLogTable.clear();
        metadataTable.clear();
        bucketsTable.clear();
        multipartTable.clear();
    }
}
