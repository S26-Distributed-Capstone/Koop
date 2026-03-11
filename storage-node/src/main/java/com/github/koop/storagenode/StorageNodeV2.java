package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.OpLog;
import com.github.koop.storagenode.db.Operation;

public class StorageNodeV2 {
    private final Database db;
    private final Path storageDir;

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

        Path path = getObjectPath(key);
        Files.createDirectories(path.getParent());
        write(path, data, length);
    }

    protected void commit(int partition, String key, String requestID, long seqNumber) throws Exception {
        db.atomicallyUpdate(new Metadata(key, requestID, partition, seqNumber), new OpLog(seqNumber, key, Operation.PUT));
    }

    protected void delete(int partition, String key, String requestID, long seqNumber) throws Exception {
        db.atomicallyUpdate(new Metadata(key, Database.TOMBSTONE_LOCATION, partition, seqNumber), new OpLog(seqNumber, key, Operation.DELETE));
    }

    protected Optional<ObjectAndMeta> retrieve(String key) throws Exception {
        var metaOpt = db.getMetadata(key);
        if (metaOpt.isEmpty() || metaOpt.get().location().equals(Database.TOMBSTONE_LOCATION)) {
            return Optional.empty();
        }
        var meta = metaOpt.get();
        Path path = getObjectPath(meta.location());
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        InputStream data = Files.newInputStream(path, StandardOpenOption.READ);
        return Optional.of(new ObjectAndMeta(data, metaOpt.get()));
    }



    private final void write(Path path, ReadableByteChannel data, long length) throws IOException {
        try (FileChannel fc = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            long written = 0;
            while (written < length) {
                long n = fc.transferFrom(data, written, length - written);
                if(!data.isOpen()){
                    throw new IOException("Data channel closed before expected length was written");
                }
                else if (n == 0) {
                    // TCP buffer is empty (waiting on network). Sleep 1ms to yield the Virtual
                    // Thread.
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Transfer interrupted", e);
                    }
                } else if (n < 0) {
                    throw new EOFException("Unexpected EOF reading payload");
                } else {
                    written += n;
                }
            }
            fc.force(true);

        }
    }
}