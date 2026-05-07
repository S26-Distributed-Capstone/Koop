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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private WriteTracker writeTracker;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() throws Exception {
        db = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
        writeTracker = new WriteTracker();
        storageNode = new StorageNodeV2(db, tempDir, writeTracker);
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

        storageNode.store(partition, key, requestID, Channels.newChannel(new ByteArrayInputStream(requestData)));
        storageNode.commit(partition, key, requestID, seqNumber, requestData.length);

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

        Optional<GetObjectResponse> specificVersionOpt = storageNode.retrieve(key, seqNumber);
        assertTrue(specificVersionOpt.isPresent(), "Response for specific version should be present");
        try (FileObject vo = (FileObject) specificVersionOpt.get()) {
            assertEquals(seqNumber, vo.version().sequenceNumber());
        }

        Optional<GetObjectResponse> unknownVersionOpt = storageNode.retrieve(key, 999L);
        assertFalse(unknownVersionOpt.isPresent(), "Unknown version should not be present");
    }

    @Test
    public void testDelete() throws Exception {
        String key = "bucket/obj-to-delete";
        String requestID = "req-del";
        byte[] data = "data".getBytes();

        storageNode.store(1, key, requestID, Channels.newChannel(new ByteArrayInputStream(data)));
        storageNode.commit(1, key, requestID, 10L, data.length);

        assertTrue(storageNode.retrieve(key).isPresent());

        storageNode.delete(1, key, 11L);

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
        byte[] data = "data".getBytes();

        storageNode.store(1, key, requestID, Channels.newChannel(new ByteArrayInputStream(data)));
        storageNode.commit(1, key, requestID, 10L, data.length);

        storageNode.delete(1, key, 11L);

        byte[] newData = "newdata".getBytes();
        storageNode.store(1, key, requestID, Channels.newChannel(new ByteArrayInputStream(newData)));
        storageNode.commit(1, key, requestID, 12L, newData.length);

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
            assertArrayEquals(newData, readBytes);
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

        storageNode.multipartCommit(1, key, 50L, chunks, 5000L);

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
    public void testWriteTrackerFalseWhenIdle() {
        assertFalse(writeTracker.isActive("any-key"),
                "WriteTracker should not report active write when nothing is writing");
    }

    @Test
    public void testWriteTrackerTrueWhileStoreExecutes() throws Exception {
        String key = "bucket/concurrent-key";
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch allowComplete = new CountDownLatch(1);
        AtomicBoolean wasActiveWhileWriting = new AtomicBoolean(false);

        var executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                var blockingChannel = Channels.newChannel(new java.io.InputStream() {
                    private final byte[] data = "blocking-data".getBytes();
                    private int pos = 0;
                    @Override
                    public int read() {
                        if (pos == 0) {
                            writeStarted.countDown();
                            try { allowComplete.await(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
                        }
                        return pos < data.length ? (data[pos++] & 0xFF) : -1;
                    }
                });
                storageNode.store(1, key, "req-active", blockingChannel);
            } catch (Exception ignored) {}
        });

        assertTrue(writeStarted.await(5, TimeUnit.SECONDS), "Store should have started");
        wasActiveWhileWriting.set(writeTracker.isActive(key));
        allowComplete.countDown();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertTrue(wasActiveWhileWriting.get(), "WriteTracker should be active while store() is executing");
        assertFalse(writeTracker.isActive(key), "WriteTracker should be inactive after store() completes");
    }

    @Test
    public void testWriteTrackerFalseAfterStoreCompletes() throws Exception {
        String key = "bucket/post-store-key";
        storageNode.store(1, key, "req-post", Channels.newChannel(new ByteArrayInputStream("data".getBytes())));
        assertFalse(writeTracker.isActive(key),
                "WriteTracker should be inactive once store() returns");
    }

    @Test
    public void testCommitReturnsTrueWhenBlobPreStored() throws Exception {
        String key = "bucket/pre-stored";
        String reqId = "req-pre";
        byte[] data = "data".getBytes();
        storageNode.store(1, key, reqId, Channels.newChannel(new ByteArrayInputStream(data)));

        boolean materialized = storageNode.commit(1, key, reqId, 42L, data.length);
        assertTrue(materialized, "commit() should return true when blob was stored first");
    }

    @Test
    public void testCommitReturnsFalseWhenBlobNotYetStored() throws Exception {
        boolean materialized = storageNode.commit(1, "bucket/no-blob", "req-absent", 10L, 1024L);
        assertFalse(materialized, "commit() should return false when blob has not arrived yet");
    }

    @Test
    public void testCommitReturnsTrueForLateArrivalAfterCommit() throws Exception {
        String key = "bucket/late-arrival";
        String reqId = "req-late";
        byte[] lateData = "late".getBytes();

        boolean firstCommit = storageNode.commit(1, key, reqId, 20L, lateData.length);
        assertFalse(firstCommit, "First commit without blob should return false");

        storageNode.store(1, key, reqId, Channels.newChannel(new ByteArrayInputStream(lateData)));

        Optional<StorageNodeV2.GetObjectResponse> resp = storageNode.retrieve(key);
        assertTrue(resp.isPresent(), "Object should be retrievable after late blob arrival");
        assertTrue(resp.get() instanceof StorageNodeV2.FileObject);
    }

    @Test
    public void testRetrieveReturnEmptyForMissingBlobWithoutEnqueueingRepair() throws Exception {
        WriteTracker tracker = new WriteTracker();
        RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(
                tempDir.resolve("repair-db").toAbsolutePath().toString());
        RocksDbRepairQueue repairQueue = new RocksDbRepairQueue(strategy);
        RepairWorkerPool repairPool = new RepairWorkerPool(repairQueue, tracker, op -> {});
        repairPool.start();
        storageNode = new StorageNodeV2(db, tempDir, tracker);

        storageNode.commit(1, "bucket/ghost", "req-ghost", 99L, 1024L);

        var result = storageNode.retrieve("bucket/ghost");
        assertTrue(result.isEmpty(),
                "Missing blob should return empty");
        assertEquals(0, repairPool.pendingCount(),
                "retrieve() should not enqueue repair — only the commit path triggers repair");

        repairPool.shutdown();
        strategy.close();
    }

    @Test
    public void testRetrieveReturnsEmptyWhenPoolIsNull() throws Exception {
        storageNode.commit(1, "bucket/no-pool", "req-no-pool", 5L, 1024L);
        Optional<StorageNodeV2.GetObjectResponse> result = storageNode.retrieve("bucket/no-pool");
        assertTrue(result.isEmpty(), "Missing blob should return empty; no exception even with null repair queue");
    }

    @Test
    public void testListItemsInBucket() throws Exception {
        String prefix = "test-bucket/";
        storageNode.store(1,prefix+"file1", "req-1", Channels.newChannel(new ByteArrayInputStream("1".getBytes())));
        storageNode.commit(1, prefix + "file1", "req-1", 100L, 1L);

        storageNode.store(1, prefix+"file2","req-2", Channels.newChannel(new ByteArrayInputStream("2".getBytes())));
        storageNode.commit(1, prefix + "file2", "req-2", 101L, 1L);

        storageNode.store(1,"other-bucket/file3", "req-3", Channels.newChannel(new ByteArrayInputStream("3".getBytes())));
        storageNode.commit(1, "other-bucket/file3", "req-3", 102L, 1L);

        List<Metadata> items = storageNode.listItemsInBucket(prefix).toList();

        assertEquals(2, items.size());
        assertTrue(items.stream().anyMatch(m -> m.key().equals(prefix + "file1")));
        assertTrue(items.stream().anyMatch(m -> m.key().equals(prefix + "file2")));
    }
}