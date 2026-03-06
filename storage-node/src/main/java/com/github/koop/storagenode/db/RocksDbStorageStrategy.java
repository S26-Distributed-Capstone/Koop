package com.github.koop.storagenode.db;

import org.rocksdb.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class RocksDbStorageStrategy implements StorageStrategy {
    private final RocksDB db;
    private final List<ColumnFamilyHandle> handles;
    private final ColumnFamilyHandle logHandle;
    private final ColumnFamilyHandle metaHandle;

    public RocksDbStorageStrategy(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("op_log".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));

        handles = new ArrayList<>();
        DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        db = RocksDB.open(options, dbPath, descriptors, handles);

        logHandle = handles.get(1);
        metaHandle = handles.get(2);
    }

    @Override
    public void close() throws Exception {
        for (ColumnFamilyHandle handle : handles) {
            if (handle != null) {
                handle.close();
            }
        }
        if (db != null) {
            db.close();
        }
    }

    @Override
    public void addLog(OpLog log) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array();
        byte[] value = log.serialize();
        db.put(logHandle, key, value);
    }

    @Override
    public void updateMetadata(Metadata metadata) throws Exception {
        byte[] key = metadata.fileName().getBytes(StandardCharsets.UTF_8);
        byte[] value = metadata.serialize();
        db.put(metaHandle, key, value);
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception {
        byte[] logKey = ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array();
        byte[] logValue = log.serialize();

        byte[] metaKey = metadata.fileName().getBytes(StandardCharsets.UTF_8);
        byte[] metaValue = metadata.serialize();

        try (WriteBatch writeBatch = new WriteBatch();
                WriteOptions writeOptions = new WriteOptions()) {

            writeBatch.put(logHandle, logKey, logValue);
            writeBatch.put(metaHandle, metaKey, metaValue);

            db.write(writeOptions, writeBatch);
        }
    }

    @Override
    public Metadata getMetadata(String fileKey) throws Exception {
        byte[] key = fileKey.getBytes(StandardCharsets.UTF_8);
        byte[] value = db.get(metaHandle, key);

        if (value == null) {
            return null;
        }
        return Metadata.from(value);
    }

    @Override
    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        byte[] endKey = ByteBuffer.allocate(Long.BYTES).putLong(downTo).array();
        RocksIterator iterator = db.newIterator(logHandle);
        iterator.seekForPrev(endKey);

        return Stream.generate(() -> {
            if (!iterator.isValid()) {
                return null;
            }
            long currentSeq = ByteBuffer.wrap(iterator.key()).getLong();
            if (currentSeq < from) {
                return null;
            }
            OpLog log = OpLog.from(iterator.value());
            iterator.prev();
            return log;
        }).onClose(iterator::close)
          .takeWhile(log -> log != null);
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        RocksIterator iterator = db.newIterator(metaHandle);
        iterator.seek(prefixBytes);
        return Stream.generate(()->{
            if(!iterator.isValid()){
                return null;
            }
            byte[] key = iterator.key();
            if(!startsWith(key, prefixBytes)){
                return null;
            }
            var res = Metadata.from(iterator.value());
            //prep for next
            iterator.next();
            return res;
        }).onClose(iterator::close).takeWhile(it->it!=null);
    }

    /**
     * Helper to verify if a given byte array starts with a specific byte prefix.
     */
    private boolean startsWith(byte[] source, byte[] match) {
        if (match.length > source.length) {
            return false;
        }
        for (int i = 0; i < match.length; i++) {
            if (source[i] != match[i]) {
                return false;
            }
        }
        return true;
    }
}