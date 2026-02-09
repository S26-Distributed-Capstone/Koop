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
 * StorageNode is responsible for storing, retrieving, and deleting objects on disk.
 * It uses a versioning scheme to ensure that readers always see a consistent view of the data
 * even in the presence of concurrent writes and deletes. The "current" version is tracked
 * via a separate file, and updates to this file are done atomically to prevent readers from
 * seeing partial writes. Deletes are logical (by removing the version tracking file) and physical
 * deletes are done asynchronously to avoid blocking clients.
 * 
 * On windows, files in use cannot be deleted. To handle this, we use retry loops with backoff for both version updates and deletes.
 * On linux, these operations are typically atomic and do not require retries, but the retry logic ensures cross-platform consistency.
 * 
 * The main methods are:
 * - store(partition, requestID, key, data, length): Stores the data and updates the version atomically. Old versions are GC'd asynchronously.
 * - retrieve(partition, key): Retrieves the latest version of the data for the given key. Returns Optional.empty() if not found.
 * - delete(partition, key): Logically deletes the data by removing the version tracking file. Physical deletion is done asynchronously.
 * 
 * The directory structure on disk is as follows:
 * dataDirectory/
 *  partition_{partition}/
 *   {key}/
 *   {requestID}/
 *      data.dat
 *   current <-- contains the requestID of the latest version
 * 
 * Garbage collection:
 * - When a new version is stored, the old version's requestID is captured before the update. After the new version is committed, a background thread attempts to delete the old version's data file and its parent directory.
 * - When a delete is requested, the version tracking file is removed to logically delete the object. A background thread then attempts to delete the data file and its parent directory.
 * TODO: Full background GC
 * 
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
        return dataDirectory.resolve("partition_" + partition).resolve(key);
    }

    private Path getObjectFile(int partition, String requestID, String key) {
        return getObjectFolder(partition, key).resolve(requestID).resolve("data.dat");
    }

    private Path getVersionTrackingFile(int partition, String key) {
        return getObjectFolder(partition, key).resolve("current");
    }

    private Path getVersionTrackingTempFile(int partition, String key, String requestID) {
        return getObjectFolder(partition, key).resolve(requestID).resolve("current_temp");
    }

    private void bumpLatestObjectStored(int partition, String key, String requestID) throws IOException {
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        Path versionTrackingFileTemp = getVersionTrackingTempFile(partition, key, requestID);
        
        Files.write(versionTrackingFileTemp, requestID.getBytes());
        
        // Use retry loop to prevent crashes when file is briefly read-locked
        moveWithRetry(versionTrackingFileTemp, versionTrackingFile);
    }

    private void moveWithRetry(Path source, Path target) throws IOException {
        int retries = 10; 
        long waitMs = 5;

        for (int i = 0; i < retries; i++) {
            try {
                // ATOMIC_MOVE is key here. It ensures that once the lock is acquired,
                // the switch happens instantly.
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return; // Success
            } catch (IOException e) {
                // On Windows, AccessDeniedException is common if a reader is checking 'current'
                // If this is the last attempt, give up and rethrow.
                if (i == retries - 1) {
                    throw new IOException("Failed to update version file (locked?): " + target, e);
                }
                // Backoff and retry
                try {
                    Thread.sleep(waitMs);
                    waitMs = Math.min(100, waitMs * 2); 
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during version update", ie);
                }
            }
        }
    }

    private Optional<String> getLatestObjectStored(int partition, String key) {
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        try {
            var bytes = Files.readAllBytes(versionTrackingFile);
            if (bytes.length == 0) {
                return Optional.empty();
            }
            return Optional.of(new String(bytes));
        } catch (IOException e) {
            // If read fails (e.g. file deleted during read), treat as Not Found
            return Optional.empty();
        }
    }

    protected void store(
            int partition,
            String requestID,
            String key,
            ReadableByteChannel data, 
            long length) throws IOException {

        // 1. Capture OLD version for GC later
        Optional<String> oldVersion = getLatestObjectStored(partition, key);

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
                if (n <= 0) {
                     // Check for unexpected EOF
                     if (written < length) throw new EOFException("Unexpected EOF reading payload");
                     break;
                }
                written += n;
            }
        }

        // 2. Commit the NEW version (Atomic Switch)
        bumpLatestObjectStored(partition, key, requestID);

        // 3. GC the OLD version (Best Effort)
        Thread.ofVirtual().start(() -> {
             if (oldVersion.isPresent() && !oldVersion.get().equals(requestID)) {
                 try {
                     Path oldPath = getObjectFile(partition, oldVersion.get(), key);
                     deleteUntilSuccess(oldPath); // Helper from before
                     Files.deleteIfExists(oldPath.getParent());
                 } catch (IOException ignored) {
                     // Log and ignore
                 }
             }
        });
    }

    protected Optional<FileChannel> retrieve(int partition, String key) throws IOException {
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        // no version tracking file or empty version means no data
        if (latestObjectIDStored.isEmpty()) {
            return Optional.empty();
        }
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return Optional.of(channel);
        } catch (IOException e) {
            // if the file cannot be read, that means its being deleted, so treat as Not Found
            return Optional.empty();
        }
    }

    protected boolean delete(int partition, String key) throws IOException {
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if (latestObjectIDStored.isEmpty()) {
            return false;
        }
        
        // Logical Delete (Retry required for consistency)
        var versionTrackingFile = getVersionTrackingFile(partition, key);
        if (!deleteUntilSuccess(versionTrackingFile)) {
            return false;
        }
        
        // Physical Delete - client doesn't need to wait for this, so we can do async best-effort cleanup
        Thread.ofVirtual().start(() -> {
             try {
                 Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
                 deleteUntilSuccess(path); // Helper from before
                 Files.deleteIfExists(path.getParent());
             } catch (IOException ignored) {
                 // Log and ignore
             }
        });

        return true;
    }

    private boolean deleteUntilSuccess(Path path) throws IOException {
        long timeoutMs = 5000;
        long deadline = System.currentTimeMillis() + timeoutMs;
        long waitMs = 1;

        while (System.currentTimeMillis() < deadline) {
            try {
                Files.deleteIfExists(path);
                return true;
            } catch (IOException e) {
                try {
                    Thread.sleep(waitMs);
                    if (waitMs < 100) waitMs *= 2;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", ie);
                }
            }
        }
        return false;
    }
}