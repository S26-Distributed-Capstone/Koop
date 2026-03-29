package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.storagenode.StorageNodeV2.GetObjectResponse;
import com.github.koop.storagenode.StorageNodeV2.MultipartData;
import com.github.koop.storagenode.StorageNodeV2.Tombstone;
import com.github.koop.storagenode.StorageNodeV2.FileObject;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.MultipartFileVersion;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

public class StorageNodeV2Test {

    private Database db;
    private StorageNodeV2 storageNode;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() throws Exception {
        db = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
        storageNode = new StorageNodeV2(db, tempDir);
    }

    @AfterEach
    public void teardown() throws Exception {
        db.close();
    }

    @Test
    public void testStoreAndRetrieve() throws Exception {
        String key = "my-bucket/my-object";
        String requestID = "req-12345";
        byte[] requestData = "Hello World".getBytes();
        int partition = 1;
        long seqNumber = 100L;

        // 1. Store data
        storageNode.store(partition, requestID, Channels.newChannel(new ByteArrayInputStream(requestData)),
                requestData.length);

        // 2. Commit metadata
        storageNode.commit(partition, key, requestID, seqNumber);

        // 3. Retrieve latest
        Optional<GetObjectResponse> responseOpt = storageNode.retrieve(key);
        assertTrue(responseOpt.isPresent(), "Response should be present");

        GetObjectResponse response = responseOpt.get();
        assertTrue(response instanceof FileObject, "Expected VersionedObject");

        try (FileObject vo = (FileObject) response) {
            assertEquals(seqNumber, vo.version().sequenceNumber());

            ByteBuffer buffer = ByteBuffer.allocate(requestData.length);
            int totalRead = 0;
            int read;
            while (buffer.hasRemaining() && (read = vo.data().read(buffer)) != -1) {
                totalRead += read;
            }
            assertEquals(requestData.length, totalRead);
            buffer.flip();
            byte[] readBytes = new byte[buffer.remaining()];
            buffer.get(readBytes);
            assertArrayEquals(requestData, readBytes);
        }

        // 4. Retrieve specific version
        Optional<GetObjectResponse> specificVersionOpt = storageNode.retrieve(key, seqNumber);
        assertTrue(specificVersionOpt.isPresent(), "Response for specific version should be present");
        try (FileObject vo = (FileObject) specificVersionOpt.get()) {
            assertEquals(seqNumber, vo.version().sequenceNumber());
        }

        // 5. Retrieve an unknown version
        Optional<GetObjectResponse> unknownVersionOpt = storageNode.retrieve(key, 999L);
        assertFalse(unknownVersionOpt.isPresent(), "Unknown version should not be present");
    }

    @Test
    public void testDelete() throws Exception {
        String key = "bucket/obj-to-delete";
        String requestID = "req-del";

        // Setup existing object
        storageNode.store(1, requestID, Channels.newChannel(new ByteArrayInputStream("data".getBytes())), 4);
        storageNode.commit(1, key, requestID, 10L);

        // Ensure it's there
        assertTrue(storageNode.retrieve(key).isPresent());

        // Delete object
        storageNode.delete(1, key, 11L);

        // Retrieve should get Tombstone
        var responseOpt = storageNode.retrieve(key);
        assertTrue(responseOpt.isPresent(), "Response should be present after deletion");
        var response = responseOpt.get();
        assertTrue(response instanceof Tombstone, "Expected a Tombstone after deletion");
        Tombstone tombstone = (Tombstone) response;
        assertEquals(11L, tombstone.version().sequenceNumber());
    }

    @Test
    public void testPutAfterDelete() throws Exception {
        String key = "bucket/obj-recreate";
        String requestID = "req-recreate";

        // Setup existing object
        storageNode.store(1, requestID, Channels.newChannel(new ByteArrayInputStream("data".getBytes())), 4);
        storageNode.commit(1, key, requestID, 10L);

        // Delete object
        storageNode.delete(1, key, 11L);

        // Recreate object
        storageNode.store(1, requestID, Channels.newChannel(new ByteArrayInputStream("newdata".getBytes())), 7);
        storageNode.commit(1, key, requestID, 12L);

        // Retrieve should get new data
        var responseOpt = storageNode.retrieve(key);
        assertTrue(responseOpt.isPresent(), "Response should be present after recreation");
        var response = responseOpt.get();
        assertTrue(response instanceof FileObject, "Expected a FileObject after recreation");
        try (FileObject vo = (FileObject) response) {
            assertEquals(12L, vo.version().sequenceNumber());

            ByteBuffer buffer = ByteBuffer.allocate(7);
            int read = vo.data().read(buffer);
            assertEquals(7, read);
            buffer.flip();
            byte[] readBytes = new byte[buffer.remaining()];
            buffer.get(readBytes);
            assertArrayEquals("newdata".getBytes(), readBytes);
        }
    }

    @Test
    public void testRetrieveEmpty() throws Exception {
        assertFalse(storageNode.retrieve("non-existent-key").isPresent());
    }

    @Test
    public void testMultipartCommit() throws Exception {
        String key = "bucket/multipart-obj";
        List<String> chunks = List.of("chunk1", "chunk2", "chunk3");

        storageNode.multipartCommit(1, key, 50L, chunks);

        Optional<GetObjectResponse> responseOpt = storageNode.retrieve(key);
        assertTrue(responseOpt.isPresent());

        GetObjectResponse response = responseOpt.get();
        assertTrue(response instanceof MultipartData);

        MultipartData md = (MultipartData) response;
        MultipartFileVersion mfv = md.version();
        assertEquals(50L, mfv.sequenceNumber());
        assertEquals(chunks, mfv.chunks());
    }

    @Test
    public void testBucketOperations() throws Exception {
        String bucketKey = "my-test-bucket";

        assertFalse(storageNode.bucketExists(bucketKey));

        storageNode.createBucket(1, bucketKey, 1L);
        assertTrue(storageNode.bucketExists(bucketKey));

        storageNode.deleteBucket(1, bucketKey, 2L);
        assertFalse(storageNode.bucketExists(bucketKey));
    }

    @Test
    public void testListItemsInBucket() throws Exception {
        String prefix = "test-bucket/";
        storageNode.store(1, "req-1", Channels.newChannel(new ByteArrayInputStream("1".getBytes())), 1);
        storageNode.commit(1, prefix + "file1", "req-1", 100L);

        storageNode.store(1, "req-2", Channels.newChannel(new ByteArrayInputStream("2".getBytes())), 1);
        storageNode.commit(1, prefix + "file2", "req-2", 101L);

        storageNode.store(1, "req-3", Channels.newChannel(new ByteArrayInputStream("3".getBytes())), 1);
        storageNode.commit(1, "other-bucket/file3", "req-3", 102L);

        List<Metadata> items = storageNode.listItemsInBucket(prefix).toList();

        assertEquals(2, items.size());
        assertTrue(items.stream().anyMatch(m -> m.key().equals(prefix + "file1")));
        assertTrue(items.stream().anyMatch(m -> m.key().equals(prefix + "file2")));
    }
}
