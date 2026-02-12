package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;

public interface StorageService {
    void putObject(String bucket, String key, InputStream data, long length ) throws Exception;
    InputStream getObject(String bucket, String key) throws Exception;
    void deleteObject(String bucket, String key) throws Exception;
}
