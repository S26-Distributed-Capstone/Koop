package com.github.koop.storagenode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class StorageNodeTest {

    @TempDir
    Path tempDir;

    @Test
    void testStoreAndRetrieve() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "my-key";
        int partition = 1;
        String reqId = "req-1";
        byte[] contentBytes = "Hello World".getBytes(StandardCharsets.UTF_8);

        // Store with length
        node.store(partition, reqId, key, new ByteArrayInputStream(contentBytes), contentBytes.length);

        // Retrieve
        Optional<InputStream> result = node.retrieve(partition, key);
        assertTrue(result.isPresent(), "Data should be present");
        
        String retrievedContent = new String(result.get().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals("Hello World", retrievedContent, "Retrieved content should match stored content");
    }

    @Test
    void testOverwriteUpdatesVersion() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "key-v";
        int partition = 1;

        byte[] v1 = "Data V1".getBytes();
        byte[] v2 = "Data V2".getBytes();

        node.store(partition, "v1", key, new ByteArrayInputStream(v1), v1.length);
        node.store(partition, "v2", key, new ByteArrayInputStream(v2), v2.length);

        Optional<InputStream> result = node.retrieve(partition, key);
        assertTrue(result.isPresent());
        assertEquals("Data V2", new String(result.get().readAllBytes()));
    }

    @Test
    void testDelete() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "del-key";
        int partition = 1;
        
        byte[] data = "data".getBytes();
        node.store(partition, "req1", key, new ByteArrayInputStream(data), data.length);
        assertTrue(node.retrieve(partition, key).isPresent());

        boolean deleted = node.delete(partition, key);
        assertTrue(deleted);
        
        // Verify file is gone
        Path path = tempDir.resolve("partition_" + partition).resolve(key).resolve("req1").resolve("data.dat");
        assertFalse(Files.exists(path));
    }
}