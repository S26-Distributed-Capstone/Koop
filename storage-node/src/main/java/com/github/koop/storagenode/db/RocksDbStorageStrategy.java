package com.github.koop.storagenode.db;

import org.rocksdb.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDbStorageStrategy implements StorageStrategy {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final RocksDB db;
    private final List<ColumnFamilyHandle> handles;
    private final ColumnFamilyHandle logHandle;
    private final ColumnFamilyHandle metaHandle;

    public RocksDbStorageStrategy(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor("op_log".getBytes(), new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor("metadata".getBytes(), new ColumnFamilyOptions())
        );

        handles = new ArrayList<>();
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        
        db = RocksDB.open(options, dbPath, descriptors, handles);
        
        logHandle = handles.get(1);
        metaHandle = handles.get(2);
    }

    @Override
    public void addLog(long sequenceNumber, OpLog log) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(sequenceNumber).array();
        byte[] value = mapper.writeValueAsBytes(log);
        db.put(logHandle, key, value);
    }

    @Override
    public void updateMetadata(String fileKey, Metadata metadata) throws Exception {
        byte[] key = fileKey.getBytes();
        byte[] value = mapper.writeValueAsBytes(metadata);
        db.put(metaHandle, key, value);
    }

    @Override
    public Metadata getMetadata(String fileKey) throws Exception {
        byte[] value = db.get(metaHandle, fileKey.getBytes());
        if (value == null) return null;
        return mapper.readValue(value, Metadata.class);
    }

    @Override
    public void close() {
        // Always close handles before the DB to prevent memory leaks
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        if (db != null) {
            db.close();
        }
    }
}
