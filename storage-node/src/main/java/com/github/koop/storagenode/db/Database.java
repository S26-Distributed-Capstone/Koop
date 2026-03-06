package com.github.koop.storagenode.db;

import java.util.stream.Stream;

public class Database implements AutoCloseable {
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    public void logOperation(long sequenceNumber, String fileKey, String operation) throws Exception {
        OpLog log = new OpLog(sequenceNumber, fileKey, operation);
        strategy.addLog(log);
    }

    public void setMetadata(String fileKey, String location, String partition, long seq) throws Exception {
        Metadata meta = new Metadata(fileKey, location, partition, seq);
        strategy.updateMetadata(meta);
    }

    public void atomicallyUpdate(long sequenceNumber, String fileKey, String operation, String location, String partition) throws Exception {
        OpLog log = new OpLog(sequenceNumber, fileKey, operation);
        Metadata meta = new Metadata(fileKey, location, partition, sequenceNumber);
        strategy.atomicallyUpdateLogAndMetadata(log, meta);
    }

    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        return strategy.getLogs(from, downTo);
    }

    public Metadata getMetadata(String fileKey) throws Exception {
        return strategy.getMetadata(fileKey);
    }

    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return strategy.streamMetadataWithPrefix(prefix);
    }

    @Override
    public void close() throws Exception {
        strategy.close();
    }
}