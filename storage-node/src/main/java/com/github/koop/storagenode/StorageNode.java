package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * StorageNode is responsible for managing the storage and retrieval of data objects
 * in a partitioned directory structure on the local filesystem. Each object is identified
 * by a partition, a unique request ID, and a key. The class provides methods to store,
 * retrieve, and delete objects, as well as to track the latest stored version for each key.
 * <p>
 * The storage layout is organized as follows:
 * <pre>
 *   dataDirectory/
 *     partition_{partition}/
 *       {key}/
 *         {requestID}/
 *           data.dat
 *         current
 *         current_temp
 * </pre>
 * The {@code current} file tracks the latest request ID for a given partition and key.
 * </p>
 * <p>
 * This class is intended to be used as a backend for distributed or local storage systems
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

    private Path getObjectFolder(int partition, String key){
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

    private Path getVersionTrackingFile(int partition, String key){
        // partition/key/current
        return getObjectFolder(partition, key)
            .resolve("current");
    }

    private Path getVersionTrackingTempFile(int partition, String key, String requestID){
        // partition/key/current_temp
        return getObjectFolder(partition, key)
            .resolve(requestID)
            .resolve("current_temp");
    }

    /**
     * Stores the given data for the specified partition, requestID, and key. The data is written to a file in the storage node's data directory. The method also updates the version tracking file to keep track of the latest stored object for the given partition and key.
     * @param partition
     * @param requestID
     * @param key
     * @param data
     * @throws IOException
     */
    protected void store(int partition, String requestID, String key, InputStream data, int length) throws IOException {
        Path path = getObjectFile(partition, requestID, key);
        Files.createDirectories(path.getParent());
        pipeToFile(data, path, length);
        bumpLatestObjectStored(partition, key, requestID);
    }

    private void bumpLatestObjectStored(int partition, String key, String requestID) throws IOException{
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        Path versionTrackingFileTemp = getVersionTrackingTempFile(partition, key, requestID);
        Files.write(versionTrackingFileTemp, requestID.getBytes());
        Files.move(versionTrackingFileTemp, versionTrackingFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Optional<String> getLatestObjectStored(int partition, String key) throws IOException{
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        var bytes = Files.readAllBytes(versionTrackingFile);
        if(bytes.length == 0){
            return Optional.empty();
        }
        return Optional.of(new String(bytes));
    }

    /**
     * Retrieves the data for the specified partition and key. The method reads the version tracking file to determine the latest stored object for the given partition and key, and then returns an InputStream to read the data from the corresponding file. If no data is found for the specified partition and key, an empty Optional is returned.
     * @param partition
     * @param key
     * @return
     * @throws IOException
     */
    protected Optional<InputStream> retrieve(int partition, String key) throws IOException {
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if(latestObjectIDStored.isEmpty()){
            return Optional.empty();
        }
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        if (!Files.exists(path)) {
            throw new IOException(
                    "Data not found for partition " + partition + ", requestID " + latestObjectIDStored.get() + ", key " + key);
        }
        return Optional.of(Files.newInputStream(path));
    }

    /**
     * Deletes the data for the specified partition and key.
     * The method reads the version tracking file to determine the latest stored object for the given partition and key, and then deletes the version tracking file & corresponding data file. 
     * If no data is found for the specified partition and key, the method returns false.
     * If the deletion is successful, it returns true.
     * @param partition
     * @param key
     * @return
     * @throws IOException
     */
    protected boolean delete(int partition, String key) throws IOException{
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if(latestObjectIDStored.isEmpty()){
            return false;
        }
        //delete our pointer 
        var versionTrackingFile = getVersionTrackingFile(partition, key);
        Files.deleteIfExists(versionTrackingFile);
        //delete object file
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        return Files.deleteIfExists(path);
    }

    private static void pipeToFile(InputStream in, Path path, int length) throws IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            copyNBytes(in, out, length);
        }
    }

    private static void copyNBytes(InputStream in, OutputStream out, int length) throws IOException {
        byte[] buffer = new byte[8192];
        int totalRead = 0;
        while (totalRead < length) {
            int bytesToRead = Math.min(buffer.length, length - totalRead);
            int read = in.read(buffer, 0, bytesToRead);
            if (read == -1) {
                throw new EOFException("Expected " + length + " bytes but got " + totalRead);
            }
            out.write(buffer, 0, read);
            totalRead += read;
        }
    }

}