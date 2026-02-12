package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.koop.queryprocessor.processor.StorageWorker;

/**
 * StorageWorkerService acts as a bridge between the API Gateway and the StorageWorker.
 * 
 * This service handles incoming requests from the API Gateway and delegates them to the
 * StorageWorker for processing (routing, erasure encoding, distribution to storage nodes).
 * 
 * Each request is handled in a dedicated virtual thread for optimal concurrency and performance.
 */
public class StorageWorkerService implements StorageService {
    
    private final StorageWorker storageWorker;
    private final ExecutorService virtualThreadExecutor;
    
    /**
     * Initializes the StorageWorkerService with the configured StorageWorker.
     * 
     * The StorageWorker should be initialized with the three replica sets
     * before being passed to this constructor.
     */
    public StorageWorkerService(StorageWorker storageWorker) {
        this.storageWorker = storageWorker;
        // Create a virtual thread executor for handling requests
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
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
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void putObject(String bucket, String key, InputStream data, long length) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute in a virtual thread and wait for completion
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            try {
                return storageWorker.put(requestId, bucket, key, length, data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to store object: " + e.getMessage(), e);
            }
        }, virtualThreadExecutor);
        
        // Block until the operation completes
        boolean success = future.join();
        
        if (!success) {
            throw new RuntimeException("StorageWorker failed to store object");
        }
    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute in a virtual thread and wait for completion
        CompletableFuture<InputStream> future = CompletableFuture.supplyAsync(() -> {
            try {
                return storageWorker.get(requestId, bucket, key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve object: " + e.getMessage(), e);
            }
        }, virtualThreadExecutor);
        
        // Block until the operation completes and return the stream
        return future.join();
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        // Generate a unique request ID for this operation
        UUID requestId = UUID.randomUUID();
        
        // Execute in a virtual thread and wait for completion
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            try {
                return storageWorker.delete(requestId, bucket, key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete object: " + e.getMessage(), e);
            }
        }, virtualThreadExecutor);
        
        // Block until the operation completes
        boolean success = future.join();
        
        if (!success) {
            throw new RuntimeException("StorageWorker failed to delete object");
        }
    }
    
    /**
     * Cleanup method to shutdown the executor service.
     * Should be called when the application is shutting down.
     */
    public void shutdown() {
        virtualThreadExecutor.shutdown();
    }
}