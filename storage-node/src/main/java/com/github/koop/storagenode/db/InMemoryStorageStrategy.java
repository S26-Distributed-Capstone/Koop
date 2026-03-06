package com.github.koop.storagenode.db;

import java.util.Optional;
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
    public Optional<Metadata> getMetadata(String fileKey) {
        return Optional.ofNullable(metadataTable.get(fileKey));
    }

    @Override
    public Optional<OpLog> getLog(long seqNum){
        return Optional.ofNullable(opLogTable.get(seqNum));
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
        return opLogTable.subMap(downTo, true, from, true).reversed().values().stream();
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        return metadataTable.tailMap(prefix, true).values().stream();
    }
}
