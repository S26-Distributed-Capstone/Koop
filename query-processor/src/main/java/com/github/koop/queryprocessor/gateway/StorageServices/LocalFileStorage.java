package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.nio.file.*;

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
    public void putObject(String bucket, String key, InputStream data) throws Exception {
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
}
