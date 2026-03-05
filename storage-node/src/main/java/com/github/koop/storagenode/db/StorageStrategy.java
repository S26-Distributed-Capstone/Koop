package com.github.koop.storagenode.db;

public interface StorageStrategy extends AutoCloseable {
    void addLog(long sequenceNumber, OpLog log) throws Exception;
    void updateMetadata(String fileKey, Metadata metadata) throws Exception;
    Metadata getMetadata(String fileKey) throws Exception;
}
