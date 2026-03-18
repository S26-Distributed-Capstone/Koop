package com.github.koop.storagenode.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class Database implements AutoCloseable {
    public static final String TOMBSTONE_LOCATION = "DELETED";
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    // --- Table #1: Operation Log ---

    public void logOperation(OpLog log) throws Exception {
        strategy.addLog(log);
    }

    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        return strategy.getLogs(from, downTo);
    }

    public Optional<OpLog> getOpLog(long seqNum) throws Exception {
        return strategy.getLog(seqNum);
    }

    // --- Table #2: Metadata ---

    public void setMetadata(Metadata meta) throws Exception {
        strategy.updateMetadata(meta);
    }

    public void atomicallyUpdate(Metadata meta, OpLog log) throws Exception {
        strategy.atomicallyUpdateLogAndMetadata(log, meta);
    }

    public Optional<Metadata> getMetadata(String fileKey) throws Exception {
        return strategy.getMetadata(fileKey);
    }

    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return strategy.streamMetadataWithPrefix(prefix)
                .takeWhile(meta -> meta.key().startsWith(prefix));
    }

    // --- Table #2: File-version helpers (read-modify-write on Metadata.versions) ---

    /**
     * Replaces the version list for the given key.
     * If no Metadata row exists yet, partition defaults to 0.
     */
    public void putFileVersions(String key, int partition, List<FileVersion> versions) throws Exception {
        strategy.updateMetadata(new Metadata(key, partition, versions));
    }

    public Optional<List<FileVersion>> getFileVersions(String key) throws Exception {
        return strategy.getMetadata(key).map(Metadata::versions);
    }

    // --- Table #2: Multipart-upload helpers ---

    /**
     * Appends a {@link MultipartFileVersion} entry to the versions list for the given key.
     * Preserves any existing {@link RegularFileVersion} entries.
     */
    public void putMultipartUpload(String key, int partition, long seqNum, List<String> chunks) throws Exception {
        var existing = strategy.getMetadata(key);
        List<FileVersion> versions = existing.map(m -> new ArrayList<>(m.versions())).orElse(new ArrayList<>());
        versions.add(new MultipartFileVersion(seqNum, chunks));
        strategy.updateMetadata(new Metadata(key, partition, versions));
    }

    /**
     * Returns the chunk list of the latest {@link MultipartFileVersion} for the given key,
     * or empty if no multipart version exists.
     */
    public Optional<List<String>> getMultipartUpload(String key) throws Exception {
        return strategy.getMetadata(key)
                .map(m -> m.versions().stream()
                        .filter(v -> v instanceof MultipartFileVersion)
                        .map(v -> ((MultipartFileVersion) v).chunks())
                        .reduce((first, second) -> second) // latest multipart
                        .orElse(null))
                .filter(c -> c != null);
    }

    /**
     * Removes all {@link MultipartFileVersion} entries from the versions list for the given key.
     */
    public void deleteMultipartUpload(String key) throws Exception {
        var existing = strategy.getMetadata(key);
        if (existing.isEmpty()) return;
        Metadata m = existing.get();
        List<FileVersion> pruned = m.versions().stream()
                .filter(v -> !(v instanceof MultipartFileVersion))
                .toList();
        strategy.updateMetadata(new Metadata(m.key(), m.partition(), pruned));
    }

    // --- Table #3: Buckets ---

    public void putBucket(Bucket bucket) throws Exception {
        strategy.putBucket(bucket);
    }

    public Optional<Bucket> getBucket(String key) throws Exception {
        return strategy.getBucket(key);
    }

    public void deleteBucket(String key) throws Exception {
        strategy.deleteBucket(key);
    }

    public Stream<Bucket> streamBuckets() throws Exception {
        return strategy.streamBuckets();
    }

    // --- Lifecycle ---

    @Override
    public void close() throws Exception {
        strategy.close();
    }
}