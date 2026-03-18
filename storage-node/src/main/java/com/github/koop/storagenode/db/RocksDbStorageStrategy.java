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
    private final ColumnFamilyHandle multipartHandle;

    public RocksDbStorageStrategy(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("op_log".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("buckets".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("multipart_uploads".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));

        handles = new ArrayList<>();
        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)) {
            db = RocksDB.open(options, dbPath, descriptors, handles);

            logHandle      = handles.get(1);
            metaHandle     = handles.get(2);
            bucketsHandle  = handles.get(3);
            multipartHandle = handles.get(4);
        }
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

    // --- Table #1: Operation Log ---

    @Override
    public void addLog(OpLog log) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(log.seqNum()).array();
        byte[] value = log.serialize();
        db.put(logHandle, key, value);
    }

    @Override
    public Optional<OpLog> getLog(long seqNum) throws Exception {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(seqNum).array();
        byte[] value = db.get(logHandle, key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(OpLog.from(value));
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

    // --- Table #2: Metadata ---

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
    public Optional<Metadata> getMetadata(String fileKey) throws Exception {
        byte[] key = fileKey.getBytes(StandardCharsets.UTF_8);
        byte[] value = db.get(metaHandle, key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(Metadata.from(value));
    }

    @Override
    public Stream<Metadata> streamMetadataWithPrefix(String prefix) throws Exception {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        RocksIterator iterator = db.newIterator(metaHandle);
        iterator.seek(prefixBytes);
        return Stream.generate(() -> {
            if (!iterator.isValid()) {
                return null;
            }
            byte[] key = iterator.key();
            if (!startsWith(key, prefixBytes)) {
                return null;
            }
            var res = Metadata.from(iterator.value());
            iterator.next();
            return res;
        }).onClose(iterator::close).takeWhile(it -> it != null);
    }

    // --- Table #3: Buckets ---

    @Override
    public void putBucket(Bucket bucket) throws Exception {
        byte[] key = bucket.key().getBytes(StandardCharsets.UTF_8);
        byte[] value = bucket.serialize();
        db.put(bucketsHandle, key, value);
    }

    @Override
    public Optional<Bucket> getBucket(String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] value = db.get(bucketsHandle, keyBytes);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(Bucket.from(value));
    }

    @Override
    public void deleteBucket(String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        db.delete(bucketsHandle, keyBytes);
    }

    @Override
    public Stream<Bucket> streamBuckets() throws Exception {
        RocksIterator iterator = db.newIterator(bucketsHandle);
        iterator.seekToFirst();
        return Stream.generate(() -> {
            if (!iterator.isValid()) {
                return null;
            }
            Bucket bucket = Bucket.from(iterator.value());
            iterator.next();
            return bucket;
        }).onClose(iterator::close).takeWhile(b -> b != null);
    }

    // --- Table #4: Multipart Uploads ---

    @Override
    public void putMultipartUpload(MultipartUpload upload) throws Exception {
        byte[] key = upload.key().getBytes(StandardCharsets.UTF_8);
        byte[] value = upload.serialize();
        db.put(multipartHandle, key, value);
    }

    @Override
    public Optional<MultipartUpload> getMultipartUpload(String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] value = db.get(multipartHandle, keyBytes);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(MultipartUpload.from(value));
    }

    @Override
    public void deleteMultipartUpload(String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        db.delete(multipartHandle, keyBytes);
    }

    // --- Helpers ---

    /**
     * Returns true if {@code source} starts with the bytes in {@code match}.
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