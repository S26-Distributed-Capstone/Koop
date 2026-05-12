package com.github.koop.storagenode.db;

import java.util.Optional;

public interface StorageTransaction extends AutoCloseable {
    Optional<Metadata> getMetadata(String key) throws Exception;
    void putMetadata(Metadata metadata) throws Exception;
    void deleteMetadata(String key) throws Exception;
    void putLog(OpLog log) throws Exception;
    void deleteLog(int partition, long seqNum) throws Exception;

    Optional<Bucket> getBucket(String key) throws Exception;
    void putBucket(Bucket bucket) throws Exception;

    Optional<Long> getUncommitted(String requestId) throws Exception;
    void deleteUncommitted(String requestId) throws Exception;

    /**
     * Atomically record an intent to physically delete a blob from disk.
     * The pending-deletion entry is durably committed alongside the rest of
     * the transaction; a background worker drains the queue, deletes the
     * on-disk file, and only then removes the entry. This keeps metadata
     * cleanup and disk reclamation crash-consistent.
     */
    void enqueueBlobDeletion(String location) throws Exception;

    void commit() throws Exception;
    void rollback() throws Exception;
    @Override void close() throws Exception;
}