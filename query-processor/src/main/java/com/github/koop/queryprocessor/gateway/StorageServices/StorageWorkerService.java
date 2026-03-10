package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

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
    
    /**
     * Initializes the StorageWorkerService with the configured StorageWorker.
     * 
     * The StorageWorker should be initialized with the three replica sets
     * before being passed to this constructor.
     */
    public StorageWorkerService(StorageWorker storageWorker) {
        this.storageWorker = storageWorker;
    }
    
    /**
     * No-arg constructor for backwards compatibility.
     * WARNING: This will create a StorageWorker with null sets - needs proper initialization!
     * 
     * TODO: Remove this once Main.java is updated to pass StorageWorker instance
     */
    public StorageWorkerService() {
        // Temporary: Create StorageWorker with null sets
        // This will need to be properly initialized with node addresses
        this.storageWorker = new StorageWorker(null, null, null);
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initiateMultipartUpload'");
    }

    @Override
    public String uploadPart(String bucket, String key, String uploadId, int partNumber, InputStream data, long length)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'uploadPart'");
    }

    @Override
    public String completeMultipartUpload(String bucket, String key, String uploadId, List<CompletedPart> parts)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'completeMultipartUpload'");
    }

    @Override
    public void abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'abortMultipartUpload'");
    }
}