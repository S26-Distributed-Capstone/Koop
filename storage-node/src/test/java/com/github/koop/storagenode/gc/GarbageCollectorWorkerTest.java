package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.RegularFileVersion;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;
import com.github.koop.storagenode.db.TombstoneFileVersion;

/**
 * End-to-end test for the gossip-driven GC: drives ActiveReadTracker,
 * PartitionWatermarks and GarbageCollectorWorker against a real RocksDB-backed
 * Database to verify the two invariants from the spec:
 *
 *   1. Active GET requests prevent deletion of the version they are reading.
 *   2. Advancing the partition sequence number via new PUTs allows the
 *      watermark to advance past prior versions so they can be GC'd.
 */
public class GarbageCollectorWorkerTest {

    private static final int PARTITION = 7;
    private static final long STALE_AFTER_MS = 60_000L;

    @TempDir
    Path tempDir;

    private RocksDbStorageStrategy strategy;
    private Database db;
    private ActiveReadTracker activeReads;
    private PartitionWatermarks watermarks;
    private GarbageCollectorWorker gc;

    @BeforeEach
    public void setup() throws Exception {
        strategy = new RocksDbStorageStrategy(tempDir.resolve("db").toAbsolutePath().toString());
        db = new Database(strategy);
        activeReads = new ActiveReadTracker();
        watermarks = new PartitionWatermarks();
        IntFunction<Set<Integer>> owned = ignored -> Set.of(PARTITION);
        // GC interval is irrelevant — we drive runOnce() manually for deterministic checks.
        gc = new GarbageCollectorWorker(db, watermarks, owned,
                STALE_AFTER_MS, /*gcIntervalMs=*/3_600_000L);
    }

    @AfterEach
    public void teardown() throws Exception {
        db.close();
    }

    /** Mirrors the body of GossipService.tick() for a single owned partition. */
    private long localMin() throws Exception {
        long max = db.getMaxSeqNum(PARTITION);
        var active = activeReads.minActiveSeq(PARTITION);
        return active.isPresent() ? Math.min(max, active.getAsLong()) : max;
    }

    private void gossipSelf() throws Exception {
        watermarks.update("self", PARTITION, localMin());
    }

    @Test
    public void staleVersionRemovedWhenWatermarkAdvances() throws Exception {
        String key = "bkt/obj1";
        db.putItem(key, PARTITION, 100L, "req-v1");
        db.putItem(key, PARTITION, 200L, "req-v2");

        // No active GET → localMin == 200.
        gossipSelf();
        int removed = gc.runOnce();
        assertEquals(1, removed, "exactly the stale v1 is GC'd");

        Metadata meta = db.getItem(key).orElseThrow();
        assertEquals(1, meta.versions().size(), "only v2 remains");
        assertEquals(200L, meta.versions().get(0).sequenceNumber());
        assertTrue(meta.versions().get(0) instanceof RegularFileVersion);

        // v1 oplog entry physically gone.
        assertFalse(strategy.getLog(PARTITION, 100L).isPresent());
        assertTrue(strategy.getLog(PARTITION, 200L).isPresent());
    }

    @Test
    public void activeGetPinsWatermarkAndBlocksGc() throws Exception {
        String key = "bkt/obj2";
        db.putItem(key, PARTITION, 10L, "req-v1");
        db.putItem(key, PARTITION, 20L, "req-v2");

        try (var handle = activeReads.begin(PARTITION, 10L)) {
            gossipSelf();
            int removed = gc.runOnce();
            assertEquals(0, removed,
                    "GC must not remove v1 while a GET is reading it (watermark pinned to 10)");

            Metadata meta = db.getItem(key).orElseThrow();
            assertEquals(2, meta.versions().size(), "both versions retained");
        }

        // After the GET finishes, watermark advances and the stale v1 is reaped.
        gossipSelf();
        int removed = gc.runOnce();
        assertEquals(1, removed);
        Metadata meta = db.getItem(key).orElseThrow();
        assertEquals(1, meta.versions().size());
        assertEquals(20L, meta.versions().get(0).sequenceNumber());
    }

    @Test
    public void tombstoneIsRemovedAndMetadataDropped() throws Exception {
        String key = "bkt/to-delete";
        db.putItem(key, PARTITION, 50L, "req-v1");
        db.deleteItem(key, PARTITION, 60L);

        // Advance partition seq with an unrelated PUT so watermark > 60.
        db.putItem("bkt/other", PARTITION, 70L, "req-other");

        gossipSelf();
        int removed = gc.runOnce();
        // v1 (stale regular) + tombstone for `key` both eligible → 2 records removed.
        assertEquals(2, removed);

        Optional<Metadata> dropped = db.getItem(key);
        assertFalse(dropped.isPresent(),
                "metadata row should be dropped once all versions are GC'd");
    }

    @Test
    public void noWatermarkMeansNoGc() throws Exception {
        String key = "bkt/no-wm";
        db.putItem(key, PARTITION, 5L, "req");
        db.putItem(key, PARTITION, 6L, "req2");

        // No gossip published → no watermark for this partition → GC skips it.
        int removed = gc.runOnce();
        assertEquals(0, removed);
        assertEquals(2, db.getItem(key).orElseThrow().versions().size());
    }

    @Test
    public void multiNodeWatermarkIsMinOfNodes() throws Exception {
        String key = "bkt/multi";
        db.putItem(key, PARTITION, 100L, "req-v1");
        db.putItem(key, PARTITION, 200L, "req-v2");

        // Replace the default watermarks/gc with a clock-driven pair so we can
        // age the peer entry past the staleness window deterministically.
        AtomicLong clock = new AtomicLong(1_000L);
        watermarks = new PartitionWatermarks(clock::get);
        IntFunction<Set<Integer>> owned = ignored -> Set.of(PARTITION);
        gc = new GarbageCollectorWorker(db, watermarks, owned,
                STALE_AFTER_MS, /*gcIntervalMs=*/3_600_000L);

        // Both peers heard at t=1000 → both fresh, min is 50.
        watermarks.update("self", PARTITION, 200L);
        watermarks.update("slow-peer", PARTITION, 50L);
        int removed = gc.runOnce();
        assertEquals(0, removed, "lagging peer pins the global watermark");

        // Advance the clock past the staleness window, then refresh only self.
        // slow-peer's lastSeen (1000) falls below cutoff and is evicted, so the
        // watermark snaps up to self's 200 and stale v1 can be reaped.
        clock.set(1_000L + 2 * STALE_AFTER_MS);
        watermarks.update("self", PARTITION, 200L);
        removed = gc.runOnce();
        assertEquals(1, removed, "stale peer is evicted, watermark advances, stale version GC'd");
    }

    @Test
    public void liveLatestRegularVersionIsNeverDeleted() throws Exception {
        String key = "bkt/single";
        db.putItem(key, PARTITION, 1L, "req");

        // Advance partition seq with unrelated writes so watermark > 1.
        for (long s = 2; s <= 5; s++) {
            db.putItem("bkt/u-" + s, PARTITION, s, "rq-" + s);
        }

        gossipSelf();
        gc.runOnce();

        // Live version is the only one — it must survive even though seq=1 < watermark.
        Metadata meta = db.getItem(key).orElseThrow();
        assertEquals(1, meta.versions().size());
        assertEquals(1L, meta.versions().get(0).sequenceNumber());
        assertFalse(meta.versions().get(0) instanceof TombstoneFileVersion);
    }
}
