package com.github.koop.storagenode.db;

public class Database implements AutoCloseable {
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    public void logOperation(long sequenceNumber, String fileKey, String operation) throws Exception {
        OpLog log = new OpLog();
        log.key = fileKey;
        log.operation = operation;
        strategy.addLog(sequenceNumber, log);
    }

    public void setMetadata(String fileKey, String location, String partition, long seq) throws Exception {
        Metadata meta = new Metadata();
        meta.location = location;
        meta.partition = partition;
        meta.sequenceNumber = seq;
        strategy.updateMetadata(fileKey, meta);
    }

    public Metadata getMetadata(String fileKey) throws Exception {
        return strategy.getMetadata(fileKey);
    }

    @Override
    public void close() throws Exception {
        strategy.close();
    }
}