package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.FileVersion;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.MultipartFileVersion;
import com.github.koop.storagenode.db.OpLog;
import com.github.koop.storagenode.db.Operation;
import com.github.koop.storagenode.db.RegularFileVersion;

public class StorageNodeV2 {
    private final Database db;
    private final Path storageDir;
    private final static Logger logger = LogManager.getLogger(StorageNodeV2.class);

    public StorageNodeV2(Database db, Path dir) {
        this.db = db;
        this.storageDir = dir;
    }

    private Path getObjectPath(String id) {
        return storageDir.resolve(String.format("blobs/%s", id));
    }

    static record ObjectAndMeta(InputStream data, Metadata meta) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            data.close();
        }
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
        var existingOpt = db.getMetadata(key);
        List<FileVersion> versions = existingOpt
                .map(m -> new ArrayList<>(m.versions()))
                .orElse(new ArrayList<>());

        versions.add(new RegularFileVersion(seqNumber, requestID));
        db.atomicallyUpdate(
                new Metadata(key, partition, versions),
                new OpLog(seqNumber, key, Operation.PUT));
    }

    protected void delete(int partition, String key, String requestID, long seqNumber) throws Exception {
        var existingOpt = db.getMetadata(key);
        List<FileVersion> versions = existingOpt
                .map(m -> new ArrayList<>(m.versions()))
                .orElse(new ArrayList<>());

        versions.add(new RegularFileVersion(seqNumber, Database.TOMBSTONE_LOCATION));
        db.atomicallyUpdate(
                new Metadata(key, partition, versions),
                new OpLog(seqNumber, key, Operation.DELETE));
    }

    protected Optional<ObjectAndMeta> retrieve(String key) throws Exception {
        var metaOpt = db.getMetadata(key);
        if (metaOpt.isEmpty() || metaOpt.get().versions().isEmpty()) {
            return Optional.empty();
        }
        var meta = metaOpt.get();
        FileVersion latest = meta.versions().get(meta.versions().size() - 1);

        // Determine blob path based on version type
        if (latest instanceof RegularFileVersion r) {
            if (r.location().equals(Database.TOMBSTONE_LOCATION)) {
                return Optional.empty();
            }
            Path path = getObjectPath(r.location());
            if (!Files.exists(path)) {
                return Optional.empty();
            }
            InputStream data = Files.newInputStream(path, StandardOpenOption.READ);
            return Optional.of(new ObjectAndMeta(data, meta));
        } else if (latest instanceof MultipartFileVersion m) {
            // For multipart, the chunks are the content — retrieve the first chunk's path
            // as a representative file (actual assembly is the caller's responsibility)
            if (m.chunks().isEmpty()) {
                return Optional.empty();
            }
            Path path = getObjectPath(m.chunks().get(0));
            if (!Files.exists(path)) {
                return Optional.empty();
            }
            InputStream data = Files.newInputStream(path, StandardOpenOption.READ);
            return Optional.of(new ObjectAndMeta(data, meta));
        }

        return Optional.empty();
    }

    private void write(Path path, ReadableByteChannel data, long length) throws IOException {
        try (FileChannel fc = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            long written = 0;
            while (written < length) {
                long n = fc.transferFrom(data, written, length - written);

                if (n > 0) {
                    written += n;
                } else if (!data.isOpen()) {
                    throw new IOException("Data channel closed before expected length was written");
                } else {
                    java.nio.ByteBuffer checkBuf = java.nio.ByteBuffer.allocate(1);
                    int readBytes = data.read(checkBuf);

                    if (readBytes == -1) {
                        throw new EOFException("Unexpected EOF reading payload. Expected " + length + " bytes, but only received " + written);
                    } else if (readBytes > 0) {
                        checkBuf.flip();
                        while (checkBuf.hasRemaining()) {
                            fc.write(checkBuf, written);
                        }
                        written += readBytes;
                    } else {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Transfer interrupted", e);
                        }
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