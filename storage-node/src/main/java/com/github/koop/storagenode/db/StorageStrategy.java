package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageStrategy extends AutoCloseable {
    StorageTransaction beginTransaction() throws Exception;

    void putUncommitted(String requestId, long timestamp) throws Exception;

    // --- Table #1: Operation Log ---
    Optional<OpLog> getLog(int partition, long seqNum) throws Exception;
    Stream<OpLog> getLogs(int partition, long from, long downTo) throws Exception;

    /**
     * Forward scan of the oplog within a partition, ordered by ascending seqNum,
     * starting at {@code startSeq} (inclusive). Pass 0 for a full scan; pass a
     * persisted GC cursor to bound work to entries the GC hasn't yet examined.
     */
    Stream<OpLog> streamLogsForward(int partition, long startSeq) throws Exception;

    /** Highest seqNum present in the oplog for {@code partition}, or 0 if none. */
    long getMaxSeqNum(int partition) throws Exception;

    // --- Table #5: GC Cursor ---
    /** Lowest seqNum the GC has NOT yet examined for {@code partition}; 0 if never set. */
    long getGcCursor(int partition) throws Exception;

    /** Persist the GC cursor for {@code partition}. */
    void setGcCursor(int partition, long nextSeq) throws Exception;

    // --- Table #2: Metadata (includes version history) ---
    Optional<Metadata> getMetadata(String key) throws Exception;
    Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception;

    // --- Table #3: Buckets ---
    Optional<Bucket> getBucket(String key) throws Exception;
    Stream<Bucket> streamBuckets() throws Exception;

    // --- Table #4: Pending Blob Deletions ---
    /** Forward scan of all durably-queued blob locations awaiting on-disk deletion. */
    Stream<String> streamPendingDeletions() throws Exception;

    /** Remove a pending-deletion entry after the corresponding on-disk blob is gone. */
    void removePendingDeletion(String location) throws Exception;
}