package com.github.koop.queryprocessor.gateway.cache;

import java.util.List;

/**
 * Strategy interface for tracking in-progress multipart uploads.
 *
 * <p>Implementations must be thread-safe: multiple query-processor nodes may
 * record parts for the same uploadId concurrently.
 *
 * <p>Current implementations:
 * <ul>
 *   <li>{@link MemoryMultipartUploadCache} — in-process map, suitable for dev/unit-testing
 *   <li>RedisMultipartUploadCache — distributed, for production (introduced later)
 * </ul>
 */
public interface MultipartUploadCache {

    /**
     * Records the initiation of a new multipart upload.
     *
     * @param uploadId globally unique ID for this upload (caller-generated)
     * @param bucket   target bucket name
     * @param key      target object key
     */
    void createUpload(String uploadId, String bucket, String key);

    /**
     * Records a successfully uploaded part.
     *
     * @param uploadId   the upload this part belongs to
     * @param partNumber 1-based part index
     * @param etag       ETag returned by the storage node for this part
     * @throws IllegalArgumentException if {@code uploadId} is not known
     */
    void addPart(String uploadId, int partNumber, String etag);

    /**
     * Returns all parts recorded for a given upload, in the order they were added.
     *
     * @param uploadId the upload to query
     * @return unmodifiable list of uploaded parts; empty list if the uploadId is unknown
     */
    List<UploadedPart> getParts(String uploadId);

    /**
     * Returns {@code true} if the given uploadId was created and not yet deleted.
     */
    boolean uploadExists(String uploadId);

    /**
     * Removes all state for the given upload (called on complete or abort).
     * No-op if uploadId is not known.
     *
     * @param uploadId the upload to remove
     */
    void deleteUpload(String uploadId);

    // ─── Value type ──────────────────────────────────────────────────────────

    /**
     * An immutable snapshot of a single uploaded part.
     *
     * @param partNumber 1-based part index
     * @param etag       ETag returned by the storage node
     */
    record UploadedPart(int partNumber, String etag) {}
}

