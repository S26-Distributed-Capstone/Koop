package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import com.github.koop.queryprocessor.processor.MultipartUploadManager;
import com.github.koop.queryprocessor.processor.MultipartUploadResult;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.queryprocessor.processor.cache.CacheClient;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;

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

    // UPDATED: Map the StorageWorker's RetrievedObject to the Gateway's GetObjectResult
    @Override
    public GetObjectResult getObject(String bucket, String key) throws Exception {
        UUID requestId = UUID.randomUUID();
        StorageWorker.RetrievedObject result = storageWorker.get(requestId, bucket, key);

        if (result == null) {
            return null;
        }

        return new GetObjectResult(result.stream(), result.size());
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