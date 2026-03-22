package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.FileVersion;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.MultipartFileVersion;
import com.github.koop.storagenode.db.RegularFileVersion;
import com.github.koop.storagenode.db.TombstoneFileVersion;

public class StorageNodeV2 {
    private final Database db;
    private final Path storageDir;
    private final static Logger logger = LogManager.getLogger(StorageNodeV2.class);

    public StorageNodeV2(Database db, Path dir) {
        this.db = db;
        this.storageDir = dir;
    }

    private Path getObjectPath(String id) {
        // Use the first three characters of the ID to create a sharded subdirectory
        // structure.
        // Fallback to "000" if the ID is unexpectedly short.
        String prefixDir = (id != null && id.length() >= 3) ? id.substring(0, 3) : "000";
        return storageDir.resolve(String.format("blobs/%s/%s", prefixDir, id));
    }

    static sealed interface GetObjectResponse permits FileObject, MultipartData, Tombstone {
    }

    static record MultipartData(MultipartFileVersion version) implements GetObjectResponse {
    }

    static record FileObject(FileChannel data, FileVersion version) implements AutoCloseable, GetObjectResponse {
        @Override
        public void close() throws Exception {
            data.close();
        }

    }

    static record Tombstone(TombstoneFileVersion version) implements GetObjectResponse {

    }

    protected void store(
            int partition,
            String requestID,
            String key,
            ReadableByteChannel data,
            long length) throws IOException {

        Path path = getObjectPath(requestID);
        Files.createDirectories(path.getParent());
        write(path, data, length);
    }

    protected void commit(int partition, String key, String requestID, long seqNumber) throws Exception {
        db.putItem(key, partition, seqNumber, requestID);
    }

    protected void delete(int partition, String key, String requestID, long seqNumber) throws Exception {
        db.deleteItem(key, partition, seqNumber);
    }

    protected Optional<GetObjectResponse> retrieve(String key) throws Exception {
        return retrieve(key, -1);
    }

    protected Optional<GetObjectResponse> retrieve(String key, long seqNumber) throws Exception {
        var latestOpt = seqNumber == -1 ? db.getLatestFileVersion(key) : db.getFileVersion(key, seqNumber);
        if (latestOpt.isEmpty()) {
            return Optional.empty();
        }
        var latest = latestOpt.get();
        if (latest instanceof TombstoneFileVersion t) {
            return Optional.of(new Tombstone(t));
        } else if (latest instanceof RegularFileVersion r) {
            Path path = getObjectPath(r.location());
            if (!Files.exists(path))
                return Optional.empty();
            return Optional.of(new FileObject(FileChannel.open(path, StandardOpenOption.READ), latest));
        } else if (latest instanceof MultipartFileVersion m) {
            if (m.chunks().isEmpty())
                return Optional.empty();
            return Optional.of(new MultipartData(m));
        }

        return Optional.empty();
    }

    protected void multipartCommit(int partition, String key, String requestID, long seqNumber, List<String> chunks)
            throws Exception {
        db.putMultipartItem(key, partition, seqNumber, chunks);
    }

    protected void createBucket(int partition, String bucketKey, long seqNumber) throws Exception {
        db.createBucket(bucketKey, partition, seqNumber);
    }

    protected void deleteBucket(int partition, String bucketKey, long seqNumber) throws Exception {
        db.deleteBucket(bucketKey, partition, seqNumber);
    }

    protected boolean bucketExists(String bucketKey) throws Exception {
        return db.bucketExists(bucketKey);
    }

    protected Stream<Metadata> listItemsInBucket(String bucketPrefix) throws Exception {
        return db.listItemsInBucket(bucketPrefix);
    }

private void write(Path path, ReadableByteChannel data, long length) throws IOException {
    try (FileChannel fc = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {

        long written = 0;
        // Pre-allocate a 64KB direct buffer outside the loop for fallback reads
        java.nio.ByteBuffer fallbackBuffer = java.nio.ByteBuffer.allocateDirect(64 * 1024);

        while (written < length) {
            long n = fc.transferFrom(data, written, length - written);
            if (n > 0) {
                written += n;
            } else if (!data.isOpen()) {
                throw new IOException("Data channel closed before expected length was written");
            } else {
                fallbackBuffer.clear();

                // Cap the buffer limit to avoid reading past the expected length
                int remaining = (int) Math.min((long) fallbackBuffer.capacity(), length - written);
                fallbackBuffer.limit(remaining);

                int readBytes = data.read(fallbackBuffer);
                
                if (readBytes == -1) {
                    throw new EOFException("Unexpected EOF. Expected " + length + " bytes, got " + written);
                }
                
                // Enforce the blocking mode assumption
                assert readBytes != 0 : "Blocking channel guarantee violated: read() returned 0 bytes";

                fallbackBuffer.flip();
                while (fallbackBuffer.hasRemaining()) {
                    written += fc.write(fallbackBuffer, written);
                }
            }
        }
        fc.force(true);
    } catch (IOException e) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ex) {
            logger.error("Failed to delete file after write error: {}", path, ex);
        }
        throw e;
    }
}
}