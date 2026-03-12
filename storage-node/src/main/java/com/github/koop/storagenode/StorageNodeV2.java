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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.OpLog;
import com.github.koop.storagenode.db.Operation;

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
                
                if (n > 0) {
                    written += n;
                } else if (!data.isOpen()) {
                    throw new IOException("Data channel closed before expected length was written");
                } else {
                    // n == 0. transferFrom can return 0 if the network buffer is empty, OR if we hit EOF.
                    // To safely distinguish, we attempt to read a single byte.
                    java.nio.ByteBuffer checkBuf = java.nio.ByteBuffer.allocate(1);
                    int readBytes = data.read(checkBuf);
                    
                    if (readBytes == -1) {
                        throw new EOFException("Unexpected EOF reading payload. Expected " + length + " bytes, but only received " + written);
                    } else if (readBytes > 0) {
                        // We successfully read a byte. Write it to the file and increment our tracker.
                        checkBuf.flip();
                        while (checkBuf.hasRemaining()) {
                            fc.write(checkBuf, written);
                        }
                        written += readBytes;
                    } else {
                        // readBytes == 0. The channel is open but truly has no data available right now. Yield virtual thread.
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
            // If there's an error during write, attempt to delete the file to avoid leaving partial data
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                // Properly log the failure using Log4j2, passing the exception stack trace
                logger.error("Failed to delete file after write error: {}", path, ex);
            }
            throw e; // Rethrow the original exception
        }
    }
}