package com.github.koop.storagenode.db;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class RocksDbStorageStrategy implements StorageStrategy {
    private final TransactionDB txnDb;
    private final List<ColumnFamilyHandle> handles;
    private final ColumnFamilyHandle logHandle;
    private final ColumnFamilyHandle metaHandle;
    private final ColumnFamilyHandle bucketsHandle;
    private final ColumnFamilyHandle uncommittedHandle;
    private final ColumnFamilyHandle repairQueueHandle;
    private volatile boolean closed = false;

    public RocksDbStorageStrategy(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("op_log".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("buckets".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("uncommitted_writes".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("repair_queue".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));

        handles = new ArrayList<>();
        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
             TransactionDBOptions txnDbOptions = new TransactionDBOptions()) {
            
            txnDb = TransactionDB.open(options, txnDbOptions, dbPath, descriptors, handles);
            logHandle         = handles.get(1);
            metaHandle        = handles.get(2);
            bucketsHandle     = handles.get(3);
            uncommittedHandle = handles.get(4);
            repairQueueHandle = handles.get(5);
        }
    }

    @Override
    public StorageTransaction beginTransaction() throws Exception {
        WriteOptions writeOptions = new WriteOptions();
        Transaction txn = txnDb.beginTransaction(writeOptions);
        return new RocksDbTransaction(txn, writeOptions, logHandle, metaHandle, bucketsHandle, uncommittedHandle);
    }

    @Override
    public void putUncommitted(String requestId, long timestamp) throws Exception {
        byte[] key = requestId.getBytes(StandardCharsets.UTF_8);
        byte[] val = ByteBuffer.allocate(8).putLong(timestamp).array();
        txnDb.put(uncommittedHandle, key, val);
    }

    // --- Repair Queue ---

    public void putRepairEntry(byte[] key, byte[] value) throws RocksDBException {
        txnDb.put(repairQueueHandle, key, value);
    }

    public void deleteRepairEntry(byte[] key) throws RocksDBException {
        txnDb.delete(repairQueueHandle, key);
    }

    public RocksIterator newRepairQueueIterator() {
        return txnDb.newIterator(repairQueueHandle);
    }

    @Override
    public void close() throws Exception {
        if (closed){
            throw new IllegalStateException("StorageStrategy already closed");
        }
        for (ColumnFamilyHandle handle : handles) {
            if (handle != null) handle.close();
        }
        if (txnDb != null) txnDb.close();
        closed = true;
    }

    // --- Table #1: Operation Log ---

    private byte[] opLogKey(int partition, long seqNum) {
        return ByteBuffer.allocate(12).putInt(partition).putLong(seqNum).array();
    }

    @Override
    public Optional<OpLog> getLog(int partition, long seqNum) throws Exception {
        byte[] value = txnDb.get(logHandle, opLogKey(partition, seqNum));
        return value == null ? Optional.empty() : Optional.of(OpLog.from(value));
    }

    @Override
    public Stream<OpLog> getLogs(int partition, long from, long downTo) throws Exception {
        byte[] startKey = opLogKey(partition, from);
        RocksIterator iterator = txnDb.newIterator(logHandle);
        iterator.seekForPrev(startKey);
        return Stream.generate(() -> {
            if (!iterator.isValid()) return null;
            ByteBuffer keyBuf = ByteBuffer.wrap(iterator.key());
            int p = keyBuf.getInt();
            long seq = keyBuf.getLong();
            if (p != partition || seq < downTo) return null;
            OpLog log = OpLog.from(iterator.value());
            iterator.prev();
            return log;
        }).onClose(iterator::close).takeWhile(log -> log != null);
    }

    // --- Table #2: Metadata ---

    @Override
    public Optional<Metadata> getMetadata(String key) throws Exception {
        byte[] value = txnDb.get(metaHandle, key.getBytes(StandardCharsets.UTF_8));
        return value == null ? Optional.empty() : Optional.of(Metadata.from(value));
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        RocksIterator iterator = txnDb.newIterator(metaHandle);
        iterator.seek(prefixBytes);
        return Stream.generate(() -> {
            if (!iterator.isValid()) return null;
            if (!startsWith(iterator.key(), prefixBytes)) return null;
            Metadata m = Metadata.from(iterator.value());
            iterator.next();
            return m;
        }).onClose(iterator::close).takeWhile(m -> m != null);
    }

    // --- Table #3: Buckets ---

    @Override
    public Optional<Bucket> getBucket(String key) throws Exception {
        byte[] value = txnDb.get(bucketsHandle, key.getBytes(StandardCharsets.UTF_8));
        return value == null ? Optional.empty() : Optional.of(Bucket.from(value));
    }

    @Override
    public Stream<Bucket> streamBuckets() throws Exception {
        RocksIterator iterator = txnDb.newIterator(bucketsHandle);
        iterator.seekToFirst();
        return Stream.generate(() -> {
            if (!iterator.isValid()) return null;
            Bucket b = Bucket.from(iterator.value());
            iterator.next();
            return b;
        }).onClose(iterator::close).takeWhile(b -> b != null);
    }

    // --- Helpers ---

    private boolean startsWith(byte[] source, byte[] match) {
        if (match.length > source.length) return false;
        for (int i = 0; i < match.length; i++) {
            if (source[i] != match[i]) return false;
        }
        return true;
    }

    // --- Transaction Implementation ---

    private static class RocksDbTransaction implements StorageTransaction {
        private final Transaction txn;
        private final WriteOptions writeOptions;
        private final ColumnFamilyHandle logHandle;
        private final ColumnFamilyHandle metaHandle;
        private final ColumnFamilyHandle bucketsHandle;
        private final ColumnFamilyHandle uncommittedHandle;
        private final ReadOptions readOptions;

        public RocksDbTransaction(Transaction txn, WriteOptions writeOptions, 
                                  ColumnFamilyHandle logHandle, ColumnFamilyHandle metaHandle,
                                  ColumnFamilyHandle bucketsHandle, ColumnFamilyHandle uncommittedHandle) {
            this.txn = txn;
            this.writeOptions = writeOptions;
            this.logHandle = logHandle;
            this.metaHandle = metaHandle;
            this.bucketsHandle = bucketsHandle;
            this.uncommittedHandle = uncommittedHandle;
            this.readOptions = new ReadOptions();
        }

        @Override
        public Optional<Metadata> getMetadata(String key) throws Exception {
            byte[] value = txn.get(readOptions, metaHandle, key.getBytes(StandardCharsets.UTF_8));
            return value == null ? Optional.empty() : Optional.of(Metadata.from(value));
        }

        @Override
        public void putMetadata(Metadata metadata) throws Exception {
            txn.put(metaHandle, metadata.key().getBytes(StandardCharsets.UTF_8), metadata.serialize());
        }

        @Override
        public void putLog(OpLog log) throws Exception {
            byte[] key = ByteBuffer.allocate(12).putInt(log.partition()).putLong(log.seqNum()).array();
            txn.put(logHandle, key, log.serialize());
        }

        @Override
        public Optional<Bucket> getBucket(String key) throws Exception {
            byte[] value = txn.get(readOptions, bucketsHandle, key.getBytes(StandardCharsets.UTF_8));
            return value == null ? Optional.empty() : Optional.of(Bucket.from(value));
        }

        @Override
        public void putBucket(Bucket bucket) throws Exception {
            txn.put(bucketsHandle, bucket.key().getBytes(StandardCharsets.UTF_8), bucket.serialize());
        }

        @Override
        public Optional<Long> getUncommitted(String requestId) throws Exception {
            byte[] value = txn.get(readOptions, uncommittedHandle, requestId.getBytes(StandardCharsets.UTF_8));
            return value == null ? Optional.empty() : Optional.of(ByteBuffer.wrap(value).getLong());
        }

        @Override
        public void deleteUncommitted(String requestId) throws Exception {
            txn.delete(uncommittedHandle, requestId.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void commit() throws Exception {
            txn.commit();
        }

        @Override
        public void rollback() throws Exception {
            txn.rollback();
        }

        @Override
        public void close() throws Exception {
            readOptions.close();
            writeOptions.close();
            txn.close();
        }
    }
}