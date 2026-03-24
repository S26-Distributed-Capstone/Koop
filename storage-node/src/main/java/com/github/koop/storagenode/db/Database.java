package com.github.koop.storagenode.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * High-level database facade exposing exactly the operations the storage node
 * needs.
 */
public class Database implements AutoCloseable {
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    // -------------------------------------------------------------------------
    // PUT item — atomic write into metadata and oplog
    // -------------------------------------------------------------------------

    /** Commits a regular (single-blob) version of {@code key}. */
    public void putItem(String key, int partition, long seqNumber, String location) throws Exception {
        List<FileVersion> versions = currentVersions(key);
        versions.add(new RegularFileVersion(seqNumber, location));
        strategy.atomicallyUpdateLogAndMetadata(
                new OpLog(partition, seqNumber, key, Operation.PUT),
                new Metadata(key, partition, versions));
    }

    /** Commits a completed multipart version of {@code key}. */
    public void putMultipartItem(String key, int partition, long seqNumber, List<String> chunks) throws Exception {
        List<FileVersion> versions = currentVersions(key);
        versions.add(new MultipartFileVersion(seqNumber, chunks));
        strategy.atomicallyUpdateLogAndMetadata(
                new OpLog(partition, seqNumber, key, Operation.PUT),
                new Metadata(key, partition, versions));
    }

    // -------------------------------------------------------------------------
    // DELETE item — atomic tombstone write into metadata and oplog
    // -------------------------------------------------------------------------

    public void deleteItem(String key, int partition, long seqNumber) throws Exception {
        List<FileVersion> versions = currentVersions(key);
        versions.add(new TombstoneFileVersion(seqNumber));
        strategy.atomicallyUpdateLogAndMetadata(
                new OpLog(partition, seqNumber, key, Operation.DELETE),
                new Metadata(key, partition, versions));
    }

    // -------------------------------------------------------------------------
    // GET ITEM — return metadata row
    // -------------------------------------------------------------------------

    public Optional<Metadata> getItem(String key) throws Exception {
        return strategy.getMetadata(key);
    }

    public Optional<FileVersion> getLatestFileVersion(String key) throws Exception {
        var versions = currentVersions(key);
        if (versions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(versions.getLast());
    }

    public Optional<FileVersion> getFileVersion(String key, long seqNumber) throws Exception {
        var versions = currentVersions(key);
        if (versions.isEmpty()) {
            return Optional.empty();
        }
        return versions.stream().filter(v -> v.sequenceNumber() == seqNumber).findFirst();
    }

    // -------------------------------------------------------------------------
    // CREATE BUCKET — atomic write into bucket table and oplog
    // -------------------------------------------------------------------------

    public void createBucket(String bucketKey, int partition, long seqNumber) throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(partition, seqNumber, bucketKey, Operation.CREATE_BUCKET),
                new Bucket(bucketKey, partition, seqNumber, false));
    }

    // -------------------------------------------------------------------------
    // DELETE BUCKET — atomic tombstone write into bucket table and oplog
    // -------------------------------------------------------------------------

    public void deleteBucket(String bucketKey, int partition, long seqNumber) throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(partition, seqNumber, bucketKey, Operation.DELETE_BUCKET),
                new Bucket(bucketKey, partition, seqNumber, true));
    }

    // -------------------------------------------------------------------------
    // BUCKET EXISTS — check bucket table (false for tombstoned rows)
    // -------------------------------------------------------------------------

    public boolean bucketExists(String bucketKey) throws Exception {
        return strategy.getBucket(bucketKey)
                .map(b -> !b.deleted())
                .orElse(false);
    }

    // -------------------------------------------------------------------------
    // LIST ITEMS IN BUCKET — prefix scan on metadata table
    // -------------------------------------------------------------------------

    public Stream<Metadata> listItemsInBucket(String bucketPrefix) throws Exception {
        return strategy.streamMetadataWithPrefix(bucketPrefix);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        strategy.close();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private List<FileVersion> currentVersions(String key) throws Exception {
        return strategy.getMetadata(key)
                .map(m -> new ArrayList<>(m.versions()))
                .orElse(new ArrayList<>());
    }
}