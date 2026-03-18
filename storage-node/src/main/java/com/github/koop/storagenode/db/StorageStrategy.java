package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageStrategy extends AutoCloseable {
    // --- Table #1: Operation Log ---
    void addLog(OpLog log) throws Exception;
    Optional<OpLog> getLog(long seqNum) throws Exception;
    Stream<OpLog> getLogs(long from, long downTo) throws Exception;

    // --- Table #2: Metadata ---
    void updateMetadata(Metadata metadata) throws Exception;
    void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception;
    Optional<Metadata> getMetadata(String fileKey) throws Exception;
    Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception;

    // --- Table #3: Buckets ---
    void putBucket(Bucket bucket) throws Exception;
    Optional<Bucket> getBucket(String key) throws Exception;
    void deleteBucket(String key) throws Exception;
    Stream<Bucket> streamBuckets() throws Exception;

    // --- Table #4: Multipart Uploads ---
    void putMultipartUpload(MultipartUpload upload) throws Exception;
    Optional<MultipartUpload> getMultipartUpload(String key) throws Exception;
    void deleteMultipartUpload(String key) throws Exception;
}
