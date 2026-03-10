package com.github.koop.storagenode.db;

import java.util.Optional;
import java.util.stream.Stream;

public interface StorageStrategy extends AutoCloseable {
    void addLog(OpLog log) throws Exception;

    void updateMetadata(Metadata metadata) throws Exception;

    void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception;

    Optional<Metadata> getMetadata(String fileKey) throws Exception;

    Optional<OpLog> getLog(long seqNum) throws Exception;

    Stream<OpLog> getLogs(long from, long downTo) throws Exception;

    Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception;
}
