package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.nio.file.*;
import java.util.List;

public class LocalFileStorage implements StorageService {

    private final Path rootDir;

    public LocalFileStorage(String baseDir){
        this.rootDir = Paths.get(baseDir);
        try{
            Files.createDirectories(rootDir);
        }catch(Exception e){
            throw new RuntimeException("Failed to create root storage directory", e);
        }
    }

    @Override
    public void putObject(String bucket, String key, InputStream data, long length) throws Exception {
        Path bucketPath = rootDir.resolve(bucket);
        Files.createDirectories(bucketPath);
        Path dest = bucketPath.resolve(key);
        Files.copy(data, dest, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("[LocalFileStorage] Saved: " + dest);   
    }
    
    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        Path filePath = rootDir.resolve(bucket).resolve(key);
        if(Files.exists(filePath)){
            System.out.println("[LocalFileStorage] Retrieved: " + filePath);
            return Files.newInputStream(filePath);
        }
        return null; // Or throw exception
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        Path filePath = rootDir.resolve(bucket).resolve(key);
        Files.deleteIfExists(filePath);
        System.out.println("[LocalFileStorage] Deleted: " + filePath);
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
