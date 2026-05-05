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
 * StorageWorkerService acts as a bridge between the API Gateway and the StorageWorker.
 * 
 * This service handles incoming requests from the API Gateway and delegates them to the
 * StorageWorker for processing (routing, erasure encoding, distribution to storage nodes).
 * 
 * Requests are handled directly in Javalin's virtual threads for optimal performance.
 */
public class StorageWorkerService implements StorageService {
    
    private final StorageWorker storageWorker;
    private final MultipartUploadManager multipartManager;
    
    /**
     * Initializes the StorageWorkerService with the configured StorageWorker.
     * 
     * The StorageWorker should be initialized with the three replica sets
     * before being passed to this constructor.
     */
    public StorageWorkerService(StorageWorker storageWorker) {
        this(storageWorker, new MemoryCacheClient()); // Use in-memory cache by default
    }

    /**
     * Constructor with injectable cache implementation for tests.
     */
    public StorageWorkerService(StorageWorker storageWorker, CacheClient cache) {
        this.storageWorker = storageWorker;
        this.multipartManager = new MultipartUploadManager(storageWorker, cache);
    }

    @Override
    public StorageResult putObject(String bucket, String key, InputStream data, long length) throws Exception {
        UUID requestId = UUID.randomUUID();
        boolean success = storageWorker.put(requestId, bucket, key, length, data);
        if (!success) {
            return StorageResult.failure("ServiceUnavailable",
                    "Storage backend could not reach quorum for PutObject. Please try again.", 503);
        }
        return StorageResult.success();
    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();

        // Deleted vs never-written is resolved inside StorageWorker and surfaced
        // here as null for a unified not-found contract.
        return storageWorker.get(requestId, bucket, key);
    }

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

    @Override
    public List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception {
        String bucketPrefix = bucket + "/";
        return storageWorker.listObjects(bucket, prefix, maxKeys).stream()
                .map(o -> {
                    String key = o.key();
                    if (key.startsWith(bucketPrefix)) {
                        key = key.substring(bucketPrefix.length());
                    }
                    return new ObjectSummary(key, o.size(), o.lastModified());
                })
                .toList();
    }

    @Override
    public boolean bucketExists(String bucket) throws Exception {
        return storageWorker.bucketExists(bucket);
    }

    @Override
    public String initiateMultipartUpload(String bucket, String key) throws Exception {
        return multipartManager.initiateMultipartUpload(bucket, key);
    }

    @Override
    public MultipartUploadResult uploadPart(String bucket, String key, String uploadId, int partNumber, InputStream data, long length)
            throws Exception {
        return multipartManager.uploadPart(bucket, key, uploadId, partNumber, data, length);
    }

    @Override
    public MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId, List<CompletedPart> parts)
            throws Exception {
        return multipartManager.completeMultipartUpload(bucket, key, uploadId, parts);
    }

    @Override
    public MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        return multipartManager.abortMultipartUpload(bucket, key, uploadId);
    }
}