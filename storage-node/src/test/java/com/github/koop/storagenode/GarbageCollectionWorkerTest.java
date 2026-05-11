package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

class GarbageCollectionWorkerTest {

    @TempDir
    Path tempDir;

    private Database database;
    private StorageNodeV2 storageNode;
    private ActiveSequenceTracker tracker;
    private GossipService gossip;
    private GarbageCollectionWorker worker;
    private PubSubClient pubSub;
    private WriteTracker writeTracker;

    @BeforeEach
    void setUp() throws Exception {
        database = new Database(new RocksDbStorageStrategy(
                tempDir.resolve("rocksdb").toAbsolutePath().toString()));
        writeTracker = new WriteTracker();
        storageNode = new StorageNodeV2(database, tempDir, writeTracker);
        tracker = new ActiveSequenceTracker();
        pubSub = new PubSubClient(new MemoryPubSub());
        pubSub.start();
        gossip = new GossipService("test-node", pubSub, tracker, 60_000);
        gossip.start();
        worker = new GarbageCollectionWorker(database, storageNode, gossip, writeTracker, 60_000);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (worker != null) worker.shutdown();
        if (gossip != null) gossip.stop();
        if (database != null) database.close();
    }

    /** Materialize an empty file at the location the storage node would have written it. */
    private void touchBlob(String blobId) throws Exception {
        Path p = storageNode.resolveBlobPath(blobId);
        Files.createDirectories(p.getParent());
        Files.createFile(p);
    }

    private boolean blobExists(String blobId) {
        return Files.exists(storageNode.resolveBlobPath(blobId));
    }

    @Test
    void noWatermarkMeansNothingIsDeleted() throws Exception {
        database.putItem("photos/cat", 1, 10L, "blob-10");
        touchBlob("blob-10");

        // No partition gossip yet → worker must be a no-op
        int deleted = worker.runOnce();

        assertEquals(0, deleted);
        assertTrue(blobExists("blob-10"));
        assertEquals(1, database.getItem("photos/cat").orElseThrow().versions().size());
    }

    @Test
    void purgesStaleRegularVersionAndDeletesBlobFile() throws Exception {
        database.putItem("photos/cat", 1, 5L, "blob-5");
        database.putItem("photos/cat", 1, 20L, "blob-20");
        touchBlob("blob-5");
        touchBlob("blob-20");

        tracker.recordProcessedSeq(1, 20L);
        gossip.publishOwnWatermark();

        int deleted = worker.runOnce();

        assertEquals(1, deleted, "Exactly the stale blob file should be removed");
        assertFalse(blobExists("blob-5"), "Stale blob file must be deleted from disk");
        assertTrue(blobExists("blob-20"), "Live blob file must be preserved");

        var versions = database.getItem("photos/cat").orElseThrow().versions();
        assertEquals(1, versions.size());
        assertEquals(20L, versions.get(0).sequenceNumber());
    }

    @Test
    void preservesVersionsAtAndAboveWatermark() throws Exception {
        database.putItem("k", 1, 10L, "blob-10");
        database.putItem("k", 1, 20L, "blob-20");
        touchBlob("blob-10");
        touchBlob("blob-20");

        tracker.recordProcessedSeq(1, 10L);
        gossip.publishOwnWatermark();

        worker.runOnce();

        assertTrue(blobExists("blob-10"),
                "Version equal to the watermark must be retained");
        assertTrue(blobExists("blob-20"));
        assertEquals(2, database.getItem("k").orElseThrow().versions().size());
    }

    @Test
    void purgesTombstonedRowEntirelyWhenWatermarkPasses() throws Exception {
        database.putItem("ephemeral", 1, 5L, "blob-eph");
        database.deleteItem("ephemeral", 1, 6L);
        touchBlob("blob-eph");

        tracker.recordProcessedSeq(1, 100L);
        gossip.publishOwnWatermark();

        worker.runOnce();

        assertFalse(blobExists("blob-eph"));
        assertTrue(database.getItem("ephemeral").isEmpty(),
                "Both the regular version and tombstone marker must be compacted away");
    }

    @Test
    void purgesMultipartChunksOnDisk() throws Exception {
        database.putMultipartItem("video", 1, 5L,
                List.of("chunk-A", "chunk-B", "chunk-C"));
        database.putItem("video", 1, 30L, "blob-30");
        touchBlob("chunk-A");
        touchBlob("chunk-B");
        touchBlob("chunk-C");
        touchBlob("blob-30");

        tracker.recordProcessedSeq(1, 30L);
        gossip.publishOwnWatermark();

        int deleted = worker.runOnce();

        assertEquals(3, deleted, "Each multipart chunk file should be removed");
        assertFalse(blobExists("chunk-A"));
        assertFalse(blobExists("chunk-B"));
        assertFalse(blobExists("chunk-C"));
        assertTrue(blobExists("blob-30"));
    }

    @Test
    void activeGetPinsVersionAndPreventsItsCollection() throws Exception {
        database.putItem("k", 1, 5L, "blob-5");
        database.putItem("k", 1, 20L, "blob-20");
        touchBlob("blob-5");
        touchBlob("blob-20");

        tracker.recordProcessedSeq(1, 20L);
        // A GET is in flight serving version 5 — that must drag the watermark
        // down and protect version 5 from collection.
        tracker.beginGet(1, 5L);
        gossip.publishOwnWatermark();

        worker.runOnce();

        assertTrue(blobExists("blob-5"),
                "Active GET must protect the version it is currently serving");
        assertTrue(blobExists("blob-20"));
        assertEquals(2, database.getItem("k").orElseThrow().versions().size());

        // Once the GET completes, the watermark advances and GC catches up.
        tracker.endGet(1, 5L);
        gossip.publishOwnWatermark();
        worker.runOnce();

        assertFalse(blobExists("blob-5"));
        var versions = database.getItem("k").orElseThrow().versions();
        assertEquals(1, versions.size());
        assertEquals(20L, versions.get(0).sequenceNumber());
    }

    @Test
    void respectsPerPartitionWatermarks() throws Exception {
        // Two keys on different partitions; only partition 1 has a watermark.
        database.putItem("k1", 1, 5L, "blob-p1-5");
        database.putItem("k1", 1, 10L, "blob-p1-10");
        database.putItem("k2", 2, 5L, "blob-p2-5");
        database.putItem("k2", 2, 10L, "blob-p2-10");
        touchBlob("blob-p1-5");
        touchBlob("blob-p1-10");
        touchBlob("blob-p2-5");
        touchBlob("blob-p2-10");

        tracker.recordProcessedSeq(1, 10L);
        // partition 2 has no reported watermark — must be left untouched
        gossip.publishOwnWatermark();

        worker.runOnce();

        assertFalse(blobExists("blob-p1-5"));
        assertTrue(blobExists("blob-p1-10"));
        assertTrue(blobExists("blob-p2-5"),
                "Partition without gossip must not have versions collected");
        assertTrue(blobExists("blob-p2-10"));
    }

    @Test
    void skipsKeyWhileWriteIsInFlight() throws Exception {
        database.putItem("k", 1, 5L, "blob-5");
        touchBlob("blob-5");

        tracker.recordProcessedSeq(1, 100L);
        gossip.publishOwnWatermark();

        writeTracker.begin("k");
        try {
            worker.runOnce();
        } finally {
            writeTracker.end("k");
        }

        assertTrue(blobExists("blob-5"),
                "GC must defer collection while a write to the key is in flight");
        assertEquals(1, database.getItem("k").orElseThrow().versions().size());

        // Once the write completes, the next pass cleans up.
        worker.runOnce();
        assertFalse(blobExists("blob-5"));
    }
}
