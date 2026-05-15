package com.github.koop.queryprocessor.gateway.StorageServices;

import com.github.koop.queryprocessor.processor.MultipartUploadResult;

import java.io.InputStream;
import java.util.List;

/**
 * Interface defining the core storage operations required by the S3 API Gateway.
 * Any class implementing this (e.g., StorageWorkerService or LocalFileStorage)
 * can be plugged into the Javalin gateway.
 */
public interface StorageService {

    // ─── Object Operations ────────────────────────────────────────────────────

    /**
     * Wrapper record to return both the data stream and the exact logical size of the object.
     * This is critical: The Gateway uses this size to set the HTTP Content-Length header,
     * which prevents S3 clients (like S3 Browser) from downloading empty 0-byte files.
     */
    record GetObjectResult(InputStream data, long size) {}

    /** Metadata-only response for S3 HEAD object. */
    record HeadObjectResult(long size, String lastModified) {}

    /**
     * Uploads an object to the specified bucket.
     *
     * @param bucket The destination bucket.
     * @param key    The object key (path).
     * @param data   The raw data stream to upload.
     * @param length The exact byte length of the data.
     * @return A {@link StorageResult} indicating success or quorum failure.
     * @throws Exception If an unexpected system or network error occurs.
     */
    StorageResult putObject(String bucket, String key, InputStream data, long length) throws Exception;

    /**
     * Retrieves an object from the specified bucket.
     *
     * @param bucket The target bucket.
     * @param key    The object key to retrieve.
     * @return A {@link GetObjectResult} containing the stream and size, or {@code null} if missing/tombstoned.
     * @throws Exception If an unexpected system or network error occurs during retrieval.
     */
    GetObjectResult getObject(String bucket, String key) throws Exception;

    /**
     * Checks for an object's existence and returns its size, without downloading
     * the body. Returns {@code null} when the key is missing or tombstoned.
     */
    HeadObjectResult headObject(String bucket, String key) throws Exception;

    /**
     * Logically deletes (tombstones) an object.
     *
     * @param bucket The target bucket.
     * @param key    The object key to delete.
     * @return A {@link StorageResult} indicating success or quorum failure.
     * @throws Exception If an unexpected system or network error occurs.
     */
    StorageResult deleteObject(String bucket, String key) throws Exception;

    // ─── Bucket Operations ────────────────────────────────────────────────────

    /**
     * Creates a new bucket across the cluster.
     *
     * @param bucket The name of the bucket to create.
     * @return A {@link StorageResult} indicating success or quorum failure.
     * @throws Exception If an unexpected system or network error occurs.
     */
    StorageResult createBucket(String bucket) throws Exception;

    /**
     * Deletes an existing bucket across the cluster.
     *
     * @param bucket The name of the bucket to delete.
     * @return A {@link StorageResult} indicating success or quorum failure.
     * @throws Exception If an unexpected system or network error occurs.
     */
    StorageResult deleteBucket(String bucket) throws Exception;

    /**
     * Lists objects within a bucket, optionally filtered by a prefix.
     *
     * @param bucket  The target bucket to scan.
     * @param prefix  An optional string to filter keys (e.g., "logs/"). Pass empty string for no prefix.
     * @param maxKeys The maximum number of keys to return in the list.
     * @return A list of {@link ObjectSummary} records representing the found objects.
     * @throws Exception If an unexpected system or network error occurs.
     */
    List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception;

    /**
     * Checks if a specific bucket exists by querying the cluster.
     *
     * @param bucket The name of the bucket to check.
     * @return {@code true} if the bucket exists; {@code false} otherwise.
     * @throws Exception If an unexpected system or network error occurs.
     */
    boolean bucketExists(String bucket) throws Exception;

    // ─── Multipart Upload Operations ──────────────────────────────────────────

    /**
     * Starts a multipart session and returns a unique upload ID.
     *
     * @param bucket The target bucket.
     * @param key    The target object key.
     * @return A unique UUID string representing the upload session.
     * @throws Exception If an unexpected system or cache error occurs.
     */
    String initiateMultipartUpload(String bucket, String key) throws Exception;

    /**
     * Uploads a single chunk of a multipart upload.
     *
     * @param bucket     The target bucket.
     * @param key        The target object key.
     * @param uploadId   The unique session ID generated by {@code initiateMultipartUpload}.
     * @param partNumber The sequential number of this part (e.g., 1, 2, 3).
     * @param data       The input stream containing this chunk's data.
     * @param length     The byte length of this chunk.
     * @return A {@link MultipartUploadResult} indicating the status of the chunk upload.
     * @throws Exception If an unexpected system or network error occurs.
     */
    MultipartUploadResult uploadPart(String bucket, String key, String uploadId,
                                     int partNumber, InputStream data, long length) throws Exception;

    /**
     * Assembles all uploaded parts into a final, logical file and commits it to storage.
     *
     * @param bucket   The target bucket.
     * @param key      The target object key.
     * @param uploadId The unique session ID for the multipart upload.
     * @param parts    A list of {@link CompletedPart} records defining the final assembly order.
     * @return A {@link MultipartUploadResult} indicating the status of the final assembly.
     * @throws Exception If an unexpected system or network error occurs.
     */
    MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId,
                                                  List<CompletedPart> parts) throws Exception;

    /**
     * Cancels an upload session and cleans up temporary chunk files.
     *
     * @param bucket   The target bucket.
     * @param key      The target object key.
     * @param uploadId The unique session ID to abort.
     * @return A {@link MultipartUploadResult} indicating success of the abort process.
     * @throws Exception If an unexpected system or network error occurs.
     */
    MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) throws Exception;

    // ─── Value types ─────────────────────────────────────────────────────────

    /** Represents a file's metadata for S3 ListObjects responses. */
    record ObjectSummary(String key, long size, String lastModified) {}

    /** Represents an uploaded chunk ready for final assembly. */
    record CompletedPart(int partNumber) {}
}