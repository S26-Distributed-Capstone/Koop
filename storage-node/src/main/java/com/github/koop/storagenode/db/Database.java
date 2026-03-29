package com.github.koop.storagenode.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class Database implements AutoCloseable {
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    public void putUncommittedWrite(String requestID, long timestamp) throws Exception {
        strategy.putUncommitted(requestID, timestamp);
    }

    // -------------------------------------------------------------------------
    // PUT item — atomic write into metadata and oplog
    // -------------------------------------------------------------------------

    public void putItem(String key, int partition, long seqNumber, String requestID) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            boolean materialized = false;
            Optional<Long> uncommitted = txn.getUncommitted(requestID);
            
            if (uncommitted.isPresent()) {
                materialized = true;
                txn.deleteUncommitted(requestID);
            }

            List<FileVersion> versions = txn.getMetadata(key)
                    .map(m -> new ArrayList<>(m.versions()))
                    .orElseGet(ArrayList::new);
            
            versions.add(new RegularFileVersion(seqNumber, requestID, materialized));
            
            txn.putLog(new OpLog(partition, seqNumber, key, Operation.PUT));
            txn.putMetadata(new Metadata(key, partition, versions));
            
            txn.commit();
        }
    }

    public void putMultipartItem(String key, int partition, long seqNumber, List<String> chunks) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            List<FileVersion> versions = txn.getMetadata(key)
                    .map(m -> new ArrayList<>(m.versions()))
                    .orElseGet(ArrayList::new);
            
            versions.add(new MultipartFileVersion(seqNumber, chunks));
            
            txn.putLog(new OpLog(partition, seqNumber, key, Operation.PUT));
            txn.putMetadata(new Metadata(key, partition, versions));
            
            txn.commit();
        }
    }

    // -------------------------------------------------------------------------
    // DELETE item — atomic tombstone write into metadata and oplog
    // -------------------------------------------------------------------------

    public void deleteItem(String key, int partition, long seqNumber) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            List<FileVersion> versions = txn.getMetadata(key)
                    .map(m -> new ArrayList<>(m.versions()))
                    .orElseGet(ArrayList::new);
            
            versions.add(new TombstoneFileVersion(seqNumber));
            
            txn.putLog(new OpLog(partition, seqNumber, key, Operation.DELETE));
            txn.putMetadata(new Metadata(key, partition, versions));
            
            txn.commit();
        }
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
        try (StorageTransaction txn = strategy.beginTransaction()) {
            txn.putLog(new OpLog(partition, seqNumber, bucketKey, Operation.CREATE_BUCKET));
            txn.putBucket(new Bucket(bucketKey, partition, seqNumber, false));
            txn.commit();
        }
    }

    // -------------------------------------------------------------------------
    // DELETE BUCKET — atomic tombstone write into bucket table and oplog
    // -------------------------------------------------------------------------

    public void deleteBucket(String bucketKey, int partition, long seqNumber) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            txn.putLog(new OpLog(partition, seqNumber, bucketKey, Operation.DELETE_BUCKET));
            txn.putBucket(new Bucket(bucketKey, partition, seqNumber, true));
            txn.commit();
        }
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