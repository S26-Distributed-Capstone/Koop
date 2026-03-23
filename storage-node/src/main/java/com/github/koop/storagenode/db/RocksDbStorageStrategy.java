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
    private final RocksDB db;
    private final List<ColumnFamilyHandle> handles;
    private final ColumnFamilyHandle logHandle;
    private final ColumnFamilyHandle metaHandle;
    private final ColumnFamilyHandle bucketsHandle;

    public RocksDbStorageStrategy(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("op_log".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("buckets".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));

        handles = new ArrayList<>();
        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)) {
            db = RocksDB.open(options, dbPath, descriptors, handles);
            logHandle     = handles.get(1);
            metaHandle    = handles.get(2);
            bucketsHandle = handles.get(3);
        }
    }

    @Override
    public void close() throws Exception {
        for (ColumnFamilyHandle handle : handles) {
            if (handle != null) handle.close();
        }
        if (db != null) db.close();
    }

    // --- Table #1: Operation Log ---

    @Override
    public void addLog(OpLog log) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array();
        db.put(logHandle, key, log.serialize());
    }

    @Override
    public Optional<OpLog> getLog(long seqNum) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(seqNum).array();
        byte[] value = db.get(logHandle, key);
        return value == null ? Optional.empty() : Optional.of(OpLog.from(value));
    }

    @Override
    public Stream<OpLog> getLogs(long from, long downTo) throws Exception {
        byte[] endKey = ByteBuffer.allocate(Long.BYTES).putLong(downTo).array();
        RocksIterator iterator = db.newIterator(logHandle);
        iterator.seekForPrev(endKey);
        return Stream.generate(() -> {
            if (!iterator.isValid()) return null;
            long seq = ByteBuffer.wrap(iterator.key()).getLong();
            if (seq < from) return null;
            OpLog log = OpLog.from(iterator.value());
            iterator.prev();
            return log;
        }).onClose(iterator::close).takeWhile(log -> log != null);
    }

    // --- Table #2: Metadata ---

    @Override
    public void updateMetadata(Metadata metadata) throws Exception {
        byte[] key = metadata.key().getBytes(StandardCharsets.UTF_8);
        db.put(metaHandle, key, metadata.serialize());
    }

    @Override
    public void atomicallyUpdateLogAndMetadata(OpLog log, Metadata metadata) throws Exception {
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            batch.put(logHandle,
                    ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array(),
                    log.serialize());
            batch.put(metaHandle,
                    metadata.key().getBytes(StandardCharsets.UTF_8),
                    metadata.serialize());
            db.write(opts, batch);
        }
    }

    @Override
    public Optional<Metadata> getMetadata(String key) throws Exception {
        byte[] value = db.get(metaHandle, key.getBytes(StandardCharsets.UTF_8));
        return value == null ? Optional.empty() : Optional.of(Metadata.from(value));
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        RocksIterator iterator = db.newIterator(metaHandle);
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
    public void updateBucket(Bucket bucket) throws Exception {
        byte[] key = bucket.key().getBytes(StandardCharsets.UTF_8);
        db.put(bucketsHandle, key, bucket.serialize());
    }

    @Override
    public void atomicallyUpdateLogAndBucket(OpLog log, Bucket bucket) throws Exception {
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            batch.put(logHandle,
                    ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array(),
                    log.serialize());
            batch.put(bucketsHandle,
                    bucket.key().getBytes(StandardCharsets.UTF_8),
                    bucket.serialize());
            db.write(opts, batch);
        }
    }

    @Override
    public Optional<Bucket> getBucket(String key) throws Exception {
        byte[] value = db.get(bucketsHandle, key.getBytes(StandardCharsets.UTF_8));
        return value == null ? Optional.empty() : Optional.of(Bucket.from(value));
    }

    @Override
    public Stream<Bucket> streamBuckets() throws Exception {
        RocksIterator iterator = db.newIterator(bucketsHandle);
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
}