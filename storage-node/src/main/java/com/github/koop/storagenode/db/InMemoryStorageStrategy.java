package com.github.koop.storagenode.db;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryStorageStrategy implements StorageStrategy {
    private final ConcurrentSkipListMap<Long, OpLog> opLogTable = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Metadata> metadataTable = new ConcurrentSkipListMap<>();

    @Override
    public void addLog(OpLog log) {
        opLogTable.put(log.seqNum(),log);
    }

    @Override
    public void updateMetadata(Metadata metadata) {
        metadataTable.put(metadata.fileName(), metadata);
    }

    @Override
    public Metadata getMetadata(String fileKey) {
        return metadataTable.get(fileKey);
    }

    @Override
    public void close() {
        opLogTable.clear();
        metadataTable.clear();
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata)
            throws Exception {
        addLog(log);
        updateMetadata(metadata);
    }

    @Override
    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        return opLogTable.subMap(from, true, downTo, true).reversed().values().stream();
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return metadataTable.tailMap(prefix, true).values().stream();
    }
}
