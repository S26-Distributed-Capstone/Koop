package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public class Database implements AutoCloseable {
    public static final String TOMBSTONE_LOCATION = "DELETED";
    private final StorageStrategy strategy;

    public Database(StorageStrategy strategy) {
        this.strategy = strategy;
    }

    public void logOperation(OpLog log) throws Exception {
        strategy.addLog(log);
    }

    public void setMetadata(Metadata meta) throws Exception {
        strategy.updateMetadata(meta);
    }

    public void atomicallyUpdate(Metadata meta, OpLog log) throws Exception {
        strategy.atomicallyUpdateLogAndMetadata(log, meta);
    }

    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        return strategy.getLogs(from, downTo);
    }

    public Optional<Metadata> getMetadata(String fileKey) throws Exception {
        return strategy.getMetadata(fileKey);
    }

    public Optional<OpLog> getOpLog(long seqNum) throws Exception {
        return strategy.getLog(seqNum);
    }

    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return strategy.streamMetadataWithPrefix(prefix)
                .takeWhile(meta -> meta.fileName().startsWith(prefix));
    }

    @Override
    public void close() throws Exception {
        strategy.close();
    }
}