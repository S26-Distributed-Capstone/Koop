package com.github.koop.storagenode.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Database implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(Database.class);
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Result of garbage-collecting a single version entry.
     *
     * @param deletedMetadata true if the row's metadata was removed because no versions remain
     */
    public record GcResult(boolean deletedMetadata) {}

    /** Maximum oplog seqNum recorded for {@code partition}, or 0 if none. */
    public long getMaxSeqNum(int partition) throws Exception {
        return strategy.getMaxSeqNum(partition);
    }

    /** Forward scan of the oplog within a partition. */
    public Stream<OpLog> streamOpLog(int partition) throws Exception {
        return strategy.streamLogsForward(partition);
    }

    /**
     * Attempt to GC a specific (partition, seqNum) version of {@code key}.
     *
     * <p>Rules:
     * <ul>
     *   <li>If the version is the latest live (regular or multipart) version, keep everything
     *       — active live versions must never be removed.</li>
     *   <li>If the version is a tombstone (regardless of position) or a stale (non-latest)
     *       regular/multipart version, remove it from metadata and remove the oplog entry.</li>
     *   <li>If no versions remain after removal, drop the metadata row entirely.</li>
     * </ul>
     */
    public Optional<GcResult> gcCleanupVersion(String key, int partition, long seqNum) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            Optional<Metadata> metaOpt = txn.getMetadata(key);
            if (metaOpt.isEmpty()) {
                txn.deleteLog(partition, seqNum);
                txn.commit();
                return Optional.empty();
            }

            Metadata metadata = metaOpt.get();
            List<FileVersion> versions = metadata.versions();

            int idx = -1;
            for (int i = 0; i < versions.size(); i++) {
                if (versions.get(i).sequenceNumber() == seqNum) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                txn.deleteLog(partition, seqNum);
                txn.commit();
                return Optional.empty();
            }

            FileVersion target = versions.get(idx);
            boolean isLatest = idx == versions.size() - 1;
            boolean isTombstone = target instanceof TombstoneFileVersion;

            if (isLatest && !isTombstone) {
                // Active live version — retain it AND its oplog entry.
                txn.rollback();
                return Optional.empty();
            }

            List<FileVersion> newVersions = new ArrayList<>(versions);
            newVersions.remove(idx);

            boolean deletedMeta = false;
            if (newVersions.isEmpty()) {
                txn.deleteMetadata(key);
                deletedMeta = true;
            } else {
                txn.putMetadata(new Metadata(metadata.key(), metadata.partition(), newVersions));
            }
            txn.deleteLog(partition, seqNum);

            // Durably enqueue the blob for physical deletion in the same txn as the
            // metadata removal — crash between metadata commit and disk unlink would
            // otherwise leak the blob.
            if (target instanceof RegularFileVersion r) {
                txn.enqueueBlobDeletion(r.location());
            }
            txn.commit();

            logger.debug("GC removed version key={} partition={} seq={} (tombstone={} latest={} metaDeleted={})",
                    key, partition, seqNum, isTombstone, isLatest, deletedMeta);
            return Optional.of(new GcResult(deletedMeta));
        }
    }

    /** Stream all blob locations awaiting on-disk deletion. */
    public Stream<String> pendingBlobDeletions() throws Exception {
        return strategy.streamPendingDeletions();
    }

    /** Remove a pending-deletion entry after the on-disk blob has been deleted. */
    public void removePendingBlobDeletion(String location) throws Exception {
        strategy.removePendingDeletion(location);
    }

    public void putUncommittedWrite(String requestID, long timestamp) throws Exception {
        strategy.putUncommitted(requestID, timestamp);
    }

    /**
     * This method should be called when a blob arrives at the storage node, to ensure that any concurrent PUT operations for the same key will properly materialize the file version instead of leaving it as uncommitted. The logic is as follows:
     * 1. Check if there is existing metadata for the key.
     * 2. If no metadata exists, simply record the uncommitted write intent (requestID and timestamp) in the database.
     * 3. If metadata exists, check if any of the existing versions match the incoming requestID. If a match is found, it means the file version has already been committed, so we update that version to mark it as materialized. If no match is found, it means the file version has not been committed yet, so we record the uncommitted write intent as in step 2.
     * @param key
     * @param requestId
     * @param timestamp
     * @throws Exception
     */
    public void registerBlobArrival(String key, String requestId, long timestamp) throws Exception {
        try(StorageTransaction txn = strategy.beginTransaction()) {
            Optional<Metadata> metadataOpt = txn.getMetadata(key);
            if(metadataOpt.isEmpty()){
                putUncommittedWrite(requestId, timestamp);
            }else{
                //already have metadata - check if we have for this version & materialize:
                Metadata metadata = metadataOpt.get();
                var versionCommitted = metadata.versions().stream()
                        .filter(v -> {
                            if(v instanceof RegularFileVersion rfv){
                                return rfv.location().equals(requestId);
                            }
                            return false;
                        })
                        .findFirst();
                if(versionCommitted.isPresent()){
                    //already committed - materialize:
                    var presentVersion = (RegularFileVersion)versionCommitted.get();
                    long seqNum = presentVersion.sequenceNumber();
                    String location = presentVersion.location();
                    var otherVersions = metadata.versions().stream()
                            .filter(v -> v != versionCommitted.get())
                            .toList();
                    var versions = new ArrayList<FileVersion>();
                    versions.addAll(otherVersions);
                    versions.add(new RegularFileVersion(seqNum, location, true));
                    txn.putMetadata(new Metadata(key, metadata.partition(), new ArrayList<>(versions)));
                }else{
                    //not committed yet - record uncommitted write intent:
                    putUncommittedWrite(requestId, timestamp);
                }
            }
            txn.commit();
        }
    }

    // -------------------------------------------------------------------------
    // PUT item — atomic write into metadata and oplog
    // -------------------------------------------------------------------------

    public boolean putItem(String key, int partition, long seqNumber, String requestID) throws Exception {
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
            return materialized;
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