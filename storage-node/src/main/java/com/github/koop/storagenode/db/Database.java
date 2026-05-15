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
     * Result of a holistic key cleanup.
     *
     * @param versionsRemoved number of FileVersion entries (and their oplog rows) reaped
     * @param deletedMetadata true if the metadata row itself was removed because no versions remain
     */
    public record GcResult(int versionsRemoved, boolean deletedMetadata) {}

    /** Maximum oplog seqNum recorded for {@code partition}, or 0 if none. */
    public long getMaxSeqNum(int partition) throws Exception {
        return strategy.getMaxSeqNum(partition);
    }

    /**
     * Forward scan of the oplog within a partition, starting from
     * {@code startSeq} (inclusive). The GC worker passes the persisted cursor
     * here so each cycle's work is bounded to O(new operations).
     */
    public Stream<OpLog> streamOpLog(int partition, long startSeq) throws Exception {
        return strategy.streamLogsForward(partition, startSeq);
    }

    /** Lowest seqNum the GC has NOT yet examined for {@code partition}. */
    public long getGcCursor(int partition) throws Exception {
        return strategy.getGcCursor(partition);
    }

    /** Persist the GC cursor for {@code partition}. */
    public void setGcCursor(int partition, long nextSeq) throws Exception {
        strategy.setGcCursor(partition, nextSeq);
    }

    /**
     * Holistic GC of a single key, triggered when the worker visits an oplog
     * entry for {@code key} at {@code visitedSeq}.
     *
     * <p>Looks at the key's metadata and reaps every version that is either:
     * <ul>
     *   <li>a tombstone with {@code seqNum < watermark}, or</li>
     *   <li>a non-current regular/multipart version with {@code seqNum < watermark}.</li>
     * </ul>
     * The latest non-tombstone version is always retained. Each reaped
     * {@link RegularFileVersion} is durably queued for blob deletion in the
     * same transaction; oplog rows for reaped versions are removed.
     *
     * <p>Doing the cleanup by-key (rather than by-(key,seq)) is what lets the
     * GC cursor advance past oplog entries we decide to keep — when a later
     * version supersedes an older one and the worker visits the newer entry,
     * this method catches the now-stale older version even if its own oplog
     * row was already skipped by the cursor.
     */
    public Optional<GcResult> gcCleanupKey(String key, int partition, long visitedSeq, long watermark)
            throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            Optional<Metadata> metaOpt = txn.getMetadata(key);
            if (metaOpt.isEmpty()) {
                // Orphan oplog row — drop it defensively.
                txn.deleteLog(partition, visitedSeq);
                txn.commit();
                return Optional.empty();
            }

            Metadata metadata = metaOpt.get();
            List<FileVersion> versions = metadata.versions();

            // The "live" version is the latest non-tombstone; if the latest entry
            // is a tombstone the key has no live state and every version is
            // eligible for reaping (assuming it's past the watermark).
            int liveIdx = -1;
            if (!versions.isEmpty()
                    && !(versions.get(versions.size() - 1) instanceof TombstoneFileVersion)) {
                liveIdx = versions.size() - 1;
            }

            List<FileVersion> keep = new ArrayList<>(versions.size());
            List<FileVersion> toDelete = new ArrayList<>();
            for (int i = 0; i < versions.size(); i++) {
                FileVersion v = versions.get(i);
                if (i == liveIdx) {
                    keep.add(v);
                } else if (v.sequenceNumber() < watermark) {
                    toDelete.add(v);
                } else {
                    keep.add(v);
                }
            }

            if (toDelete.isEmpty()) {
                txn.rollback();
                return Optional.empty();
            }

            boolean deletedMeta = false;
            if (keep.isEmpty()) {
                txn.deleteMetadata(key);
                deletedMeta = true;
            } else {
                txn.putMetadata(new Metadata(metadata.key(), metadata.partition(), keep));
            }

            for (FileVersion v : toDelete) {
                txn.deleteLog(partition, v.sequenceNumber());
                if (v instanceof RegularFileVersion r) {
                    // Durable blob-deletion intent committed atomically with metadata change.
                    txn.enqueueBlobDeletion(r.location());
                }
            }
            txn.commit();

            logger.debug("GC reaped {} version(s) for key={} partition={} watermark={} (metaDeleted={})",
                    toDelete.size(), key, partition, watermark, deletedMeta);
            return Optional.of(new GcResult(toDelete.size(), deletedMeta));
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
                Metadata metadata = metadataOpt.get();
                var versionCommitted = metadata.versions().stream()
                        .filter(v -> v instanceof RegularFileVersion rfv && rfv.location().equals(requestId))
                        .findFirst();
                if(versionCommitted.isPresent()){
                    var presentVersion = (RegularFileVersion)versionCommitted.get();
                    long seqNum = presentVersion.sequenceNumber();
                    String location = presentVersion.location();
                    long size = presentVersion.size(); // PRESERVE SIZE

                    var otherVersions = metadata.versions().stream()
                            .filter(v -> v != versionCommitted.get())
                            .toList();
                    var versions = new ArrayList<FileVersion>();
                    versions.addAll(otherVersions);
                    versions.add(new RegularFileVersion(seqNum, location, true, size)); // APPLY SIZE
                    txn.putMetadata(new Metadata(key, metadata.partition(), new ArrayList<>(versions)));
                }else{
                    putUncommittedWrite(requestId, timestamp);
                }
            }
            txn.commit();
        }
    }

    // -------------------------------------------------------------------------
    // PUT item — atomic write into metadata and oplog
    // -------------------------------------------------------------------------

    public boolean putItem(String key, int partition, long seqNumber, String requestID, long size) throws Exception {
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

            versions.add(new RegularFileVersion(seqNumber, requestID, materialized, size)); // APPLY SIZE

            txn.putLog(new OpLog(partition, seqNumber, key, Operation.PUT));
            txn.putMetadata(new Metadata(key, partition, versions));

            txn.commit();
            return materialized;
        }
    }

    public void putMultipartItem(String key, int partition, long seqNumber, List<String> chunks, long size) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            List<FileVersion> versions = txn.getMetadata(key)
                    .map(m -> new ArrayList<>(m.versions()))
                    .orElseGet(ArrayList::new);

            versions.add(new MultipartFileVersion(seqNumber, chunks, size)); // APPLY SIZE

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
    // GET BUCKET — return raw bucket row (including tombstones)
    // -------------------------------------------------------------------------

    public Optional<Bucket> getBucket(String bucketKey) throws Exception {
        return strategy.getBucket(bucketKey);
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