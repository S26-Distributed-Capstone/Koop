package edu.yu.cs.com4020.com.koop;

import java.io.InputStream;

public interface StorageService {
    void putObject(String bucket, String key, InputStream data) throws Exception;
    InputStream getObject(String bucket, String key) throws Exception;
    void deleteObject(String bucket, String key) throws Exception;
}
