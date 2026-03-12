package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import com.github.koop.queryprocessor.gateway.cache.MemoryMultipartUploadCache;
import com.github.koop.queryprocessor.gateway.cache.MultipartUploadCache;
import com.github.koop.queryprocessor.processor.StorageWorker;

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
    private final MultipartUploadCache uploadCache;
    
    /**
     * Initializes the StorageWorkerService with the configured StorageWorker and cache.
     */
    public StorageWorkerService(StorageWorker storageWorker, MultipartUploadCache uploadCache) {
        this.storageWorker = storageWorker;
        this.uploadCache = uploadCache;
    }

    /**
     * Initializes the StorageWorkerService with the configured StorageWorker.
     * Uses an in-memory cache suitable for single-node dev/testing.
     * 
     * The StorageWorker should be initialized with the three replica sets
     * before being passed to this constructor.
     */
    public StorageWorkerService(StorageWorker storageWorker) {
        this(storageWorker, new MemoryMultipartUploadCache());
    }
    
    /**
     * No-arg constructor for backwards compatibility.
     * WARNING: This will create a StorageWorker with null sets - needs proper initialization!
     * 
     * TODO: Remove this once Main.java is updated to pass StorageWorker instance
     */
    public StorageWorkerService() {
        this(new StorageWorker(null, null, null), new MemoryMultipartUploadCache());
    }

    @Override
    public void putObject(String bucket, String key, InputStream data, long length) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute directly in the calling thread (Javalin's virtual thread)
        boolean success = storageWorker.put(requestId, bucket, key, length, data);
        
        if (!success) {
            throw new RuntimeException("StorageWorker failed to store object");
        }
    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute directly in the calling thread (Javalin's virtual thread)
        return storageWorker.get(requestId, bucket, key);
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute directly in the calling thread (Javalin's virtual thread)
        boolean success = storageWorker.delete(requestId, bucket, key);
        
        if (!success) {
            throw new RuntimeException("StorageWorker failed to delete object");
        }
    }

    @Override
    public void createBucket(String bucket) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createBucket'");
    }

    @Override
    public void deleteBucket(String bucket) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteBucket'");
    }

    @Override
    public List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'listObjects'");
    }

    @Override
    public boolean bucketExists(String bucket) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'bucketExists'");
    }

    @Override
    public String initiateMultipartUpload(String bucket, String key) throws Exception {
        String uploadId = UUID.randomUUID().toString();
        uploadCache.createUpload(uploadId, bucket, key);
        return uploadId;
    }

    @Override
    public String uploadPart(String bucket, String key, String uploadId, int partNumber, InputStream data, long length)
            throws Exception {
        // TODO: delegate actual shard write to StorageWorker once multipart storage logic is ready
        String etag = UUID.randomUUID().toString();
        uploadCache.addPart(uploadId, partNumber, etag);
        return etag;
    }

    @Override
    public String completeMultipartUpload(String bucket, String key, String uploadId, List<CompletedPart> parts)
            throws Exception {
        // Retrieve tracked parts so the storage layer can verify / assemble them
        var trackedParts = uploadCache.getParts(uploadId);
        // TODO: pass trackedParts to StorageWorker once assembly logic is ready
        uploadCache.deleteUpload(uploadId);
        // TODO: return real final ETag from assembled object
        throw new UnsupportedOperationException("CompleteMultipartUpload storage assembly not yet implemented");
    }

    @Override
    public void abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        // Retrieve tracked parts so the storage layer can discard the uploaded shards
        var trackedParts = uploadCache.getParts(uploadId);
        // TODO: instruct StorageWorker to delete each shard once abort logic is ready
        uploadCache.deleteUpload(uploadId);
    }
}