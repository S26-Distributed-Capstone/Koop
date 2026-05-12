package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;
import com.github.koop.storagenode.db.StorageTransaction;

/**
 * Verifies the disk-reconciliation loop:
 *   1. Enqueueing a blob via the GC transaction durably records the intent.
 *   2. The worker physically deletes the file and dequeues the entry.
 *   3. If the node "crashes" (DB closed) before the worker has run, the queue
 *      survives a restart and the next pass cleans up.
 *   4. End-to-end with the GC worker: removing a stale regular version enqueues
 *      its location, and the deletion worker reclaims the blob.
 */
public class BlobDeletionWorkerTest {

    private static final int PARTITION = 4;
    private static final long STALE_AFTER_MS = 60_000L;

    @TempDir
    Path tempDir;

    private RocksDbStorageStrategy strategy;
    private Database db;

    @BeforeEach
    public void setup() throws Exception {
        strategy = new RocksDbStorageStrategy(tempDir.resolve("db").toAbsolutePath().toString());
        db = new Database(strategy);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (db != null) db.close();
    }

    private Path blobPathFor(String location) {
        String prefix = location.length() >= 3 ? location.substring(0, 3) : "000";
        return tempDir.normalize().resolve("blobs").resolve(prefix).resolve(location).normalize();
    }

    private Path writeBlob(String location, String content) throws Exception {
        Path p = blobPathFor(location);
        Files.createDirectories(p.getParent());
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p;
    }

    private void enqueueDeletion(String location) throws Exception {
        try (StorageTransaction txn = strategy.beginTransaction()) {
            txn.enqueueBlobDeletion(location);
            txn.commit();
        }
    }

    private List<String> pendingSnapshot() throws Exception {
        try (var s = db.pendingBlobDeletions()) {
            return s.toList();
        }
    }

    @Test
    public void workerDrainsQueueAndDeletesFile() throws Exception {
        Path blob = writeBlob("abc-loc-1", "hello");
        enqueueDeletion("abc-loc-1");
        assertEquals(List.of("abc-loc-1"), pendingSnapshot());
        assertTrue(Files.exists(blob));

        BlobDeletionWorker worker = new BlobDeletionWorker(db, tempDir, /*intervalMs=*/3_600_000L);
        int reclaimed = worker.runOnce();

        assertEquals(1, reclaimed);
        assertFalse(Files.exists(blob), "blob removed from disk");
        assertTrue(pendingSnapshot().isEmpty(), "queue drained");
    }

    @Test
    public void missingFileStillDequeues() throws Exception {
        // Never write the file on disk — simulates a redundant or stale enqueue.
        enqueueDeletion("abc-orphan");
        assertEquals(List.of("abc-orphan"), pendingSnapshot());

        BlobDeletionWorker worker = new BlobDeletionWorker(db, tempDir, /*intervalMs=*/3_600_000L);
        int reclaimed = worker.runOnce();

        assertEquals(1, reclaimed);
        assertTrue(pendingSnapshot().isEmpty(),
                "Files.deleteIfExists treats absence as success — entry must be dequeued");
    }

    @Test
    public void queueSurvivesRestart() throws Exception {
        Path blob = writeBlob("def-restart", "payload");
        enqueueDeletion("def-restart");

        // Simulate crash: close DB before the worker runs.
        db.close();
        db = null;

        // Reopen — the pending deletion must still be in the queue.
        strategy = new RocksDbStorageStrategy(tempDir.resolve("db").toAbsolutePath().toString());
        db = new Database(strategy);
        assertEquals(List.of("def-restart"), pendingSnapshot(),
                "pending deletion persisted across DB restart");

        // Worker resumes and finishes the job.
        BlobDeletionWorker worker = new BlobDeletionWorker(db, tempDir, /*intervalMs=*/3_600_000L);
        int reclaimed = worker.runOnce();
        assertEquals(1, reclaimed);
        assertFalse(Files.exists(blob));
        assertTrue(pendingSnapshot().isEmpty());
    }

    @Test
    public void gcEnqueuesAndWorkerReclaims() throws Exception {
        // Two versions of the same key; v1 is stale, v2 is live.
        String key = "bkt/obj";
        String v1Loc = "req-v1-loc";
        String v2Loc = "req-v2-loc";

        Path v1Blob = writeBlob(v1Loc, "v1-data");
        Path v2Blob = writeBlob(v2Loc, "v2-data");

        db.putItem(key, PARTITION, 100L, v1Loc);
        db.putItem(key, PARTITION, 200L, v2Loc);

        // Drive GC for v1 directly.
        PartitionWatermarks wm = new PartitionWatermarks();
        wm.update("self", PARTITION, 200L);
        GarbageCollectorWorker gc = new GarbageCollectorWorker(db, wm,
                ignored -> Set.of(PARTITION), STALE_AFTER_MS, /*intervalMs=*/3_600_000L);
        int removed = gc.runOnce();
        assertEquals(1, removed);

        // After GC: v1's blob is queued for deletion but file is still on disk.
        assertEquals(List.of(v1Loc), pendingSnapshot());
        assertTrue(Files.exists(v1Blob));

        // Disk worker reclaims v1, leaves v2 untouched.
        BlobDeletionWorker worker = new BlobDeletionWorker(db, tempDir, /*intervalMs=*/3_600_000L);
        int reclaimed = worker.runOnce();
        assertEquals(1, reclaimed);
        assertFalse(Files.exists(v1Blob));
        assertTrue(Files.exists(v2Blob), "live version's blob must not be touched");
        assertTrue(pendingSnapshot().isEmpty());
    }
}
