package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * StorageNode is responsible for managing the storage and retrieval of data
 * objects
 * in a partitioned directory structure on the local filesystem. Each object is
 * identified
 * by a partition, a unique request ID, and a key. The class provides methods to
 * store,
 * retrieve, and delete objects, as well as to track the latest stored version
 * for each key.
 * <p>
 * The storage layout is organized as follows:
 * 
 * <pre>
 *   dataDirectory/
 *     partition_{partition}/
 *       {key}/
 *         {requestID}/
 *           data.dat
 *         current
 *         current_temp
 * </pre>
 * 
 * The {@code current} file tracks the latest request ID for a given partition
 * and key.
 * </p>
 * <p>
 * This class is intended to be used as a backend for distributed or local
 * storage systems
 * that require version tracking and atomic updates.
 * </p>
 */
public class StorageNode {
    private final Path dataDirectory;

    public StorageNode(Path dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    protected Path getDataDirectory() {
        return dataDirectory;
    }

    private Path getObjectFolder(int partition, String key) {
        // partition/key/
        return dataDirectory.resolve("partition_" + partition)
                .resolve(key);
    }

    private Path getObjectFile(int partition, String requestID, String key) {
        // partition/key/data.dat
        return getObjectFolder(partition, key)
                .resolve(requestID)
                .resolve("data.dat");
    }

    private Path getVersionTrackingFile(int partition, String key) {
        // partition/key/current
        return getObjectFolder(partition, key)
                .resolve("current");
    }

    private Path getVersionTrackingTempFile(int partition, String key, String requestID) {
        // partition/key/current_temp
        return getObjectFolder(partition, key)
                .resolve(requestID)
                .resolve("current_temp");
    }

    private void bumpLatestObjectStored(int partition, String key, String requestID) throws IOException {
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        Path versionTrackingFileTemp = getVersionTrackingTempFile(partition, key, requestID);
        Files.write(versionTrackingFileTemp, requestID.getBytes());
        Files.move(versionTrackingFileTemp, versionTrackingFile, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private Optional<String> getLatestObjectStored(int partition, String key){
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        try{
            var bytes = Files.readAllBytes(versionTrackingFile);
            if (bytes.length == 0) {
                return Optional.empty();
            }
            return Optional.of(new String(bytes));
        }catch (IOException e) {
            // If we fail to read the version tracking file, treat it as if no version is stored.
            return Optional.empty();
        }
    }

    protected void store(
            int partition,
            String requestID,
            String key,
            ReadableByteChannel data,
            long length) throws IOException {

        Path path = getObjectFile(partition, requestID, key);
        Files.createDirectories(path.getParent());

        try (FileChannel fc = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            long written = 0;
            while (written < length) {
                long n = fc.transferFrom(data, written, length - written);
                if (n == 0) {
                    throw new EOFException("Unexpected EOF while reading payload");
                }
                written += n;
            }
        }

        bumpLatestObjectStored(partition, key, requestID);
    }

    protected Optional<FileChannel> retrieve(int partition, String key) throws IOException {
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if (latestObjectIDStored.isEmpty()) {
            return Optional.empty();
        }
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return Optional.of(channel);
        } catch (IOException e) {
            // the file was deleted after reading the version tracking file, return empty to
            // indicate not found
            return Optional.empty();
        }
    }

    protected boolean delete(int partition, String key) throws IOException {
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if (latestObjectIDStored.isEmpty()) {
            return false;
        }
        // delete our pointer
        var versionTrackingFile = getVersionTrackingFile(partition, key);
        var deletedVersionFile = deleteUntilSuccess(versionTrackingFile);
        if (!deletedVersionFile) {
            return false;
        }
        // delete object file
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        deleteUntilSuccess(path);
        return true;
    }

    /**
     * Aggressively retries deletion until it succeeds or times out.
     * Ideal for Windows where readers might hold brief locks.
     */
    private boolean deleteUntilSuccess(Path path) throws IOException {
        long timeoutMs = 5000; // 5 seconds is plenty for a pointer file
        long deadline = System.currentTimeMillis() + timeoutMs;
        
        // Start with a tiny wait (yield) to catch micro-gaps quickly
        long waitMs = 1; 

        while (System.currentTimeMillis() < deadline) {
            try {
                // If returns true (deleted) or false (not found), we are done.
                // We only catch the exception (Locked).
                Files.deleteIfExists(path);
                return true;
            } catch (IOException e) {
                // File is locked. Wait for the gap.
                try {
                    Thread.sleep(waitMs);
                    // Exponential backoff up to 100ms
                    if (waitMs < 100) waitMs *= 2; 
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while deleting " + path);
                }
            }
        }
        return false;
    }

}