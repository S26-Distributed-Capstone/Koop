package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageStrategy extends AutoCloseable {
    // --- Table #1: Operation Log ---
    void addLog(OpLog log) throws Exception;
    Optional<OpLog> getLog(int partition, long seqNum) throws Exception;
    Stream<OpLog> getLogs(int partition, long from, long downTo) throws Exception;

    // --- Table #2: Metadata (includes version history) ---
    void updateMetadata(Metadata metadata) throws Exception;
    void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception;
    Optional<Metadata> getMetadata(String key) throws Exception;
    Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception;

    // --- Table #3: Buckets ---
    void updateBucket(Bucket bucket) throws Exception;
    void atomicallyUpdateLogAndBucket(OpLog log, Bucket bucket) throws Exception;
    Optional<Bucket> getBucket(String key) throws Exception;
    Stream<Bucket> streamBuckets() throws Exception;
}