package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import com.github.koop.storagenode.db.Database;

import net.bytebuddy.asm.MemberSubstitution.Substitution.Chain.Step.ForField.Read;

public class StorageNodeV2 {
    private final Database db;
    private final Path storageDir;

    public StorageNodeV2(Database db, Path dir) {
        this.db = db;
        this.storageDir = dir;
    }

    private Path getObjectPath(int partition, String id) {
        return storageDir.resolve(String.format("partition-%d/%s", partition, id));
    }

    protected void store(
            int partition,
            String requestID,
            String key,
            ReadableByteChannel data,
            long length) throws IOException {

        Path path = getObjectPath(partition, key);
        Files.createDirectories(path.getParent());
        write(path, data, length);
    }

    protected void commit(int partition, String key, String requestID, long seqNumber) throws Exception {
        db.atomicallyUpdate(seqNumber, key, requestID, key, requestID);
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
                if (n == 0) {
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

        }
    }
}