package com.github.koop.storagenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

public class StorageNode {
    private final Path dataDirectory;

    public StorageNode(Path dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    protected Path getDataDirectory() {
        return dataDirectory;
    }

    private Path getObjectFile(int partition, String requestID, String key) {
        // partition/key/data.dat
        return getObjectFolder(partition, key)
                .resolve(requestID)
                .resolve("data.dat");
    }

    private Path getVersionTrackingFile(int partition, String key){
        return getObjectFolder(partition, key)
            .resolve("current");
    }

    private Path getVersionTrackingTempFile(int partition, String key){
        return getObjectFolder(partition, key)
            .resolve("current_temp");
    }

    private Path getObjectFolder(int partition, String key){
        return dataDirectory.resolve("partition_" + partition)
            .resolve(key);
    }

    protected void store(int partition, String requestID, String key, InputStream data) throws IOException {
        Path path = getObjectFile(partition, requestID, key);
        Files.createDirectories(path.getParent());
        pipeToFile(data, path);
        bumpLatestObjectStored(partition, requestID, key);
    }

    private void bumpLatestObjectStored(int partition, String requestID, String key) throws IOException{
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        Path versionTrackingFileTemp = getVersionTrackingTempFile(partition, key);
        Files.write(versionTrackingFileTemp, requestID.getBytes());
        Files.move(versionTrackingFile, versionTrackingFileTemp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Optional<String> getLatestObjectStored(int partition, String key) throws IOException{
        Path versionTrackingFile = getVersionTrackingFile(partition, key);
        var bytes = Files.readAllBytes(versionTrackingFile);
        if(bytes.length == 0){
            return Optional.empty();
        }
        return Optional.of(new String(bytes));
    }

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

    protected boolean delete(int partition, String key) throws IOException{
        var latestObjectIDStored = getLatestObjectStored(partition, key);
        if(latestObjectIDStored.isEmpty()){
            return false;
        }
        Path path = getObjectFile(partition, latestObjectIDStored.get(), key);
        return Files.deleteIfExists(path);
    }

    private static void pipeToFile(InputStream in, Path path) throws IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            in.transferTo(out);
        }
    }

}