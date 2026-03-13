package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.List;

public interface StorageService {

    // ─── Object Operations ────────────────────────────────────────────────────

    void putObject(String bucket, String key, InputStream data, long length) throws Exception;
    InputStream getObject(String bucket, String key) throws Exception;
    void deleteObject(String bucket, String key) throws Exception;

    // ─── Bucket Operations ────────────────────────────────────────────────────

    void createBucket(String bucket) throws Exception;
    void deleteBucket(String bucket) throws Exception;

    /**
     * Returns a list of object summaries in the bucket.
     * The gateway uses this to build a ListObjectsV2 XML response.
     */
    List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception;

    /**
     * Returns true if the bucket exists, false otherwise.
     * Used for HeadBucket — the gateway maps false → 404, true → 200.
     */
    boolean bucketExists(String bucket) throws Exception;

    // ─── Multipart Upload Operations ──────────────────────────────────────────

    /**
     * Initiates a multipart upload.
     *
     * @return the opaque uploadId that the client must include in all subsequent
     *         part uploads and in the final complete/abort call.
     */
    String initiateMultipartUpload(String bucket, String key) throws Exception;

    /**
     * Uploads a single part of an in-progress multipart upload.
     *
     * @return the ETag for this part (will be sent back to the client and must
     *         be echoed in the CompleteMultipartUpload call).
     */
    String uploadPart(String bucket, String key, String uploadId,
                      int partNumber, InputStream data, long length) throws Exception;

    /**
     * Finalizes a multipart upload.
     * The caller supplies the ordered list of (partNumber, ETag) pairs exactly
     * as returned by {@link #uploadPart}.
     *
     * @return the final ETag of the assembled object.
     */
    String completeMultipartUpload(String bucket, String key, String uploadId,
                                   List<CompletedPart> parts) throws Exception;

    /**
     * Aborts a multipart upload and discards any already-uploaded parts.
     */
    void abortMultipartUpload(String bucket, String key, String uploadId) throws Exception;

    // ─── Value types ─────────────────────────────────────────────────────────

    record ObjectSummary(String key, long size, String lastModified) {}
    record CompletedPart(int partNumber) {}
}