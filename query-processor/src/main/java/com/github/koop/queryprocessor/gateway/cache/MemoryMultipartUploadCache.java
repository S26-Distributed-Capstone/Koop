package com.github.koop.queryprocessor.gateway.cache;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory implementation of {@link MultipartUploadCache}.
 *
 * <p>All operations are thread-safe. Suitable for single-node development and unit
 * testing. For distributed deployments use {@code RedisMultipartUploadCache} instead.
 */
public class MemoryMultipartUploadCache implements MultipartUploadCache {

    private final ConcurrentHashMap<String, UploadEntry> uploads = new ConcurrentHashMap<>();

    @Override
    public void createUpload(String uploadId, String bucket, String key) {
        uploads.putIfAbsent(uploadId, new UploadEntry(bucket, key));
    }

    @Override
    public void addPart(String uploadId, int partNumber, String etag) {
        UploadEntry entry = uploads.get(uploadId);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown uploadId: " + uploadId);
        }
        entry.parts().add(new UploadedPart(partNumber, etag));
    }

    @Override
    public List<UploadedPart> getParts(String uploadId) {
        UploadEntry entry = uploads.get(uploadId);
        if (entry == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(entry.parts());
    }

    @Override
    public boolean uploadExists(String uploadId) {
        return uploads.containsKey(uploadId);
    }

    @Override
    public void deleteUpload(String uploadId) {
        uploads.remove(uploadId);
    }

    // ─── Internal state holder ────────────────────────────────────────────────

    private record UploadEntry(String bucket, String key, CopyOnWriteArrayList<UploadedPart> parts) {
        UploadEntry(String bucket, String key) {
            this(bucket, key, new CopyOnWriteArrayList<>());
        }
    }
}
