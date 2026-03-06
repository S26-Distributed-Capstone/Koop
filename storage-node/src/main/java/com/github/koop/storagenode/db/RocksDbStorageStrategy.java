package com.github.koop.storagenode.db;

import org.rocksdb.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
    public void close() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public void addLog(OpLog log) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addLog'");
    }

    @Override
    public void updateMetadata(Metadata metadata) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateMetadata'");
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'atomicallyUpdateLogAndMetadata'");
    }

    @Override
    public Metadata getMetadata(String fileKey) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMetadata'");
    }

    @Override
    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLogs'");
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'streamMetadataWithPrefix'");
    }
}
