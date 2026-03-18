package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public class Database implements AutoCloseable {
    public static final String TOMBSTONE_LOCATION = "DELETED";
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    // --- Table #1: Operation Log ---

    public void logOperation(OpLog log) throws Exception {
        strategy.addLog(log);
    }

    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        return strategy.getLogs(from, downTo);
    }

    public Optional<OpLog> getOpLog(long seqNum) throws Exception {
        return strategy.getLog(seqNum);
    }

    // --- Table #2: Metadata ---

    public void setMetadata(Metadata meta) throws Exception {
        strategy.updateMetadata(meta);
    }

    public void atomicallyUpdate(Metadata meta, OpLog log) throws Exception {
        strategy.atomicallyUpdateLogAndMetadata(log, meta);
    }

    public Optional<Metadata> getMetadata(String fileKey) throws Exception {
        return strategy.getMetadata(fileKey);
    }

    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return strategy.streamMetadataWithPrefix(prefix)
                .takeWhile(meta -> meta.fileName().startsWith(prefix));
    }

    // --- Table #3: Buckets ---

    public void putBucket(Bucket bucket) throws Exception {
        strategy.putBucket(bucket);
    }

    public Optional<Bucket> getBucket(String key) throws Exception {
        return strategy.getBucket(key);
    }

    public void deleteBucket(String key) throws Exception {
        strategy.deleteBucket(key);
    }

    public Stream<Bucket> streamBuckets() throws Exception {
        return strategy.streamBuckets();
    }

    // --- Table #4: Multipart Uploads ---

    public void putMultipartUpload(MultipartUpload upload) throws Exception {
        strategy.putMultipartUpload(upload);
    }

    public Optional<MultipartUpload> getMultipartUpload(String key) throws Exception {
        return strategy.getMultipartUpload(key);
    }

    public void deleteMultipartUpload(String key) throws Exception {
        strategy.deleteMultipartUpload(key);
    }

    // --- Lifecycle ---

    @Override
    public void close() throws Exception {
        strategy.close();
    }
}