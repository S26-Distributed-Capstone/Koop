package com.github.koop.storagenode.db;

import java.util.Optional;

public interface StorageTransaction extends AutoCloseable {
    Optional<Metadata> getMetadata(String key) throws Exception;
    void putMetadata(Metadata metadata) throws Exception;
    void putLog(OpLog log) throws Exception;
    
    Optional<Bucket> getBucket(String key) throws Exception;
    void putBucket(Bucket bucket) throws Exception;

    Optional<Long> getUncommitted(String requestId) throws Exception;
    void deleteUncommitted(String requestId) throws Exception;

    void commit() throws Exception;
    void rollback() throws Exception;
    @Override void close() throws Exception;
}