package com.github.koop.storagenode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class StorageNodeTest {

    @TempDir
    Path tempDir;

    // Helper to create a channel from a String
    private ReadableByteChannel createChannel(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return Channels.newChannel(new ByteArrayInputStream(bytes));
    }

    @Test
    void testStoreAndRetrieve() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "my-key";
        int partition = 1;
        String reqId = "req-1";
        String content = "Hello World";
        int length = content.length();

        // 1. Store
        node.store(partition, reqId, key, createChannel(content), length);

        // 2. Retrieve
        Optional<FileChannel> result = node.retrieve(partition, key);
        assertTrue(result.isPresent(), "Data should be present");

        // 3. Verify Content
        try (FileChannel fileChannel = result.get()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(buffer);
            buffer.flip();
            String retrievedContent = StandardCharsets.UTF_8.decode(buffer).toString();
            
            assertEquals(content, retrievedContent, "Retrieved content should match stored content");
        }
    }

    @Test
    void testOverwriteUpdatesVersion() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "key-v";
        int partition = 1;

        String v1 = "Data V1";
        String v2 = "Data V2";

        // Store Version 1
        node.store(partition, "v1", key, createChannel(v1), v1.length());

        // Store Version 2
        node.store(partition, "v2", key, createChannel(v2), v2.length());

        // Retrieve (Should get latest)
        Optional<FileChannel> result = node.retrieve(partition, key);
        assertTrue(result.isPresent());
        
        try (FileChannel fileChannel = result.get()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(buffer);
            buffer.flip();
            String retrieved = StandardCharsets.UTF_8.decode(buffer).toString();
            assertEquals(v2, retrieved);
        }
    }

    @Test
    void testDelete() throws IOException {
        StorageNode node = new StorageNode(tempDir);
        String key = "del-key";
        int partition = 1;
        String reqId = "req1";
        String content = "delete me";
        
        // Store initial data
        node.store(partition, reqId, key, createChannel(content), content.length());
        
        // Pre-check: Ensure it exists
        assertTrue(node.retrieve(partition, key).isPresent());

        // Perform Delete
        boolean deleted = node.delete(partition, key);
        assertTrue(deleted, "Delete should return true");
        
        // Verify Logical Delete (Retrieve returns empty)
        assertTrue(node.retrieve(partition, key).isEmpty(), "Retrieve should return empty after delete");

        // Verify Physical Delete (File is actually gone from disk)
        Path path = tempDir.resolve("partition_" + partition)
                           .resolve(key)
                           .resolve(reqId)
                           .resolve("data.dat");
        assertFalse(Files.exists(path), "Physical file should be deleted");
    }
}