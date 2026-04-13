package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageStrategy extends AutoCloseable {
    StorageTransaction beginTransaction() throws Exception;

    void putUncommitted(String requestId, long timestamp) throws Exception;

    // --- Table #1: Operation Log ---
    Optional<OpLog> getLog(int partition, long seqNum) throws Exception;
    Stream<OpLog> getLogs(int partition, long from, long downTo) throws Exception;

    // --- Table #2: Metadata (includes version history) ---
    Optional<Metadata> getMetadata(String key) throws Exception;
    Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception;

    // --- Table #3: Buckets ---
    Optional<Bucket> getBucket(String key) throws Exception;
    Stream<Bucket> streamBuckets() throws Exception;
}