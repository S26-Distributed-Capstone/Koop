package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import com.github.koop.queryprocessor.processor.MultipartUploadManager;
import com.github.koop.queryprocessor.processor.MultipartUploadResult;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.queryprocessor.processor.cache.CacheClient;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;

/**
 * An adapter that bridges the S3-facing StorageService interface
 * to the internal, distributed StorageWorker and MultipartUploadManager.
 * It generates the internal UUIDs required by the Koop protocol for tracking requests.
 */
public class StorageWorkerService implements StorageService {

    private final StorageWorker storageWorker;
    private final MultipartUploadManager multipartManager;

    public StorageWorkerService(StorageWorker storageWorker) {
        this(storageWorker, new MemoryCacheClient());
    }

    public StorageWorkerService(StorageWorker storageWorker, CacheClient cache) {
        this.storageWorker = storageWorker;
        this.multipartManager = new MultipartUploadManager(storageWorker, cache);
    }

    /**
     * Generates a request ID and delegates the 2-phase PUT to the StorageWorker.
     *
     * @param bucket The destination bucket.
     * @param key    The object key (path).
     * @param data   The raw data stream to upload.
     * @param length The exact byte length of the data.
     * @return StorageResult.success() if quorum is reached, StorageResult.failure() otherwise.
     */
    @Override
    public StorageResult putObject(String bucket, String key, InputStream data, long length) throws Exception {
        UUID requestId = UUID.randomUUID(); // Generate transaction ID
        boolean success = storageWorker.put(requestId, bucket, key, length, data);
        if (!success) {
            return StorageResult.failure("ServiceUnavailable",
                    "Storage backend could not reach quorum for PutObject. Please try again.", 503);
        }
        return StorageResult.success();
    }

    /**
     * Generates a request ID and requests the reconstructed stream from the StorageWorker.
     *
     * @param bucket The target bucket.
     * @param key    The object key to retrieve.
     * @return A GetObjectResult containing the stream and logical size, or null if not found.
     */
    @Override
    public GetObjectResult getObject(String bucket, String key) throws Exception {
        UUID requestId = UUID.randomUUID();

        StorageWorker.RetrievedObject result = storageWorker.get(requestId, bucket, key);

        if (result == null) {
            return null; // Object not found or tombstoned
        }

        return new GetObjectResult(result.stream(), result.size());
    }

    /**
     * Generates a request ID and delegates the tombstone operation to the StorageWorker.
     *
     * @param bucket The target bucket.
     * @param key    The object key to delete.
     * @return StorageResult.success() if the delete is acknowledged by quorum.
     */
    @Override
    public StorageResult deleteObject(String bucket, String key) throws Exception {
        UUID requestId = UUID.randomUUID();
        boolean success = storageWorker.delete(requestId, bucket, key);
        if (!success) {
            return StorageResult.failure("ServiceUnavailable",
                    "Storage backend could not reach quorum for DeleteObject. Please try again.", 503);
        }
        return StorageResult.success();
    }

    /**
     * Generates a request ID and delegates bucket creation to the StorageWorker.
     *
     * @param bucket The name of the bucket to create.
     * @return StorageResult.success() if the creation is acknowledged by quorum.
     */
    @Override
    public StorageResult createBucket(String bucket) throws Exception {
        UUID requestId = UUID.randomUUID();
        boolean success = storageWorker.createBucket(requestId, bucket);
        if (!success) {
            return StorageResult.failure("ServiceUnavailable",
                    "Storage backend could not reach quorum for CreateBucket. Please try again.", 503);
        }
        return StorageResult.success();
    }

    /**
     * Generates a request ID and delegates bucket deletion to the StorageWorker.
     *
     * @param bucket The name of the bucket to delete.
     * @return StorageResult.success() if the deletion is acknowledged by quorum.
     */
    @Override
    public StorageResult deleteBucket(String bucket) throws Exception {
        UUID requestId = UUID.randomUUID();
        boolean success = storageWorker.deleteBucket(requestId, bucket);
        if (!success) {
            return StorageResult.failure("ServiceUnavailable",
                    "Storage backend could not reach quorum for DeleteBucket. Please try again.", 503);
        }
        return StorageResult.success();
    }

    /**
     * Delegates object listing to the StorageWorker and strips internal bucket prefixes.
     *
     * @param bucket  The target bucket to scan.
     * @param prefix  An optional string to filter keys.
     * @param maxKeys The maximum number of keys to return.
     * @return A list of ObjectSummary records formatted for the S3 XML response.
     */
    @Override
    public List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception {
        String bucketPrefix = bucket + "/";
        return storageWorker.listObjects(bucket, prefix, maxKeys).stream()
                .map(o -> {
                    String key = o.key();
                    // Strip the internal "bucket/" prefix before returning to the S3 client
                    if (key.startsWith(bucketPrefix)) {
                        key = key.substring(bucketPrefix.length());
                    }
                    return new ObjectSummary(key, o.size(), o.lastModified());
                })
                .toList();
    }

    /**
     * Delegates bucket existence verification to the StorageWorker.
     *
     * @param bucket The name of the bucket to check.
     * @return {@code true} if the bucket exists, {@code false} otherwise.
     */
    @Override
    public boolean bucketExists(String bucket) throws Exception {
        return storageWorker.bucketExists(bucket);
    }

    /**
     * Delegates multipart initialization to the MultipartUploadManager.
     */
    @Override
    public String initiateMultipartUpload(String bucket, String key) throws Exception {
        return multipartManager.initiateMultipartUpload(bucket, key);
    }

    /**
     * Delegates chunk uploading to the MultipartUploadManager.
     */
    @Override
    public MultipartUploadResult uploadPart(String bucket, String key, String uploadId, int partNumber, InputStream data, long length)
            throws Exception {
        return multipartManager.uploadPart(bucket, key, uploadId, partNumber, data, length);
    }

    /**
     * Delegates final multipart assembly to the MultipartUploadManager.
     */
    @Override
    public MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId, List<CompletedPart> parts)
            throws Exception {
        return multipartManager.completeMultipartUpload(bucket, key, uploadId, parts);
    }

    /**
     * Delegates multipart abort cleanup to the MultipartUploadManager.
     */
    @Override
    public MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        return multipartManager.abortMultipartUpload(bucket, key, uploadId);
    }
}