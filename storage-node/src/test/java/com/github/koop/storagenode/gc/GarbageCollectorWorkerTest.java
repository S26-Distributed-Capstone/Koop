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
        db.putItem(key, PARTITION, 100L, "req-v1", 0L);
        db.putItem(key, PARTITION, 200L, "req-v2", 0L);

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
        db.putItem(key, PARTITION, 10L, "req-v1", 0L);
        db.putItem(key, PARTITION, 20L, "req-v2", 0L);

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
        db.putItem(key, PARTITION, 50L, "req-v1", 0L);
        db.deleteItem(key, PARTITION, 60L);

        // Advance partition seq with an unrelated PUT so watermark > 60.
        db.putItem("bkt/other", PARTITION, 70L, "req-other", 0L);

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
        db.putItem(key, PARTITION, 5L, "req", 0L);
        db.putItem(key, PARTITION, 6L, "req2", 0L);

        // No gossip published → no watermark for this partition → GC skips it.
        int removed = gc.runOnce();
        assertEquals(0, removed);
        assertEquals(2, db.getItem(key).orElseThrow().versions().size());
    }

    @Test
    public void multiNodeWatermarkIsMinOfNodes() throws Exception {
        String key = "bkt/multi";
        db.putItem(key, PARTITION, 100L, "req-v1", 0L);
        db.putItem(key, PARTITION, 200L, "req-v2", 0L);

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
    public void cursorPreventsRescanOfHistoricalOplog() throws Exception {
        // Write a stale chain so there is real work to do on the first pass.
        String key = "bkt/cur";
        db.putItem(key, PARTITION, 100L, "req-v1", 0L);
        db.putItem(key, PARTITION, 200L, "req-v2", 0L);

        gossipSelf();
        assertEquals(0L, db.getGcCursor(PARTITION), "cursor starts unset");

        int removed = gc.runOnce();
        assertEquals(1, removed, "first pass reaps stale v1");
        long cursorAfterFirst = db.getGcCursor(PARTITION);
        assertEquals(200L, cursorAfterFirst,
                "cursor advances to the watermark (=localMin=max seq=200)");

        // No new writes since last pass — gossipSelf again, then runOnce. Worker
        // should see cursor == watermark and short-circuit without iterating.
        gossipSelf();
        removed = gc.runOnce();
        assertEquals(0, removed, "second pass does nothing — cursor already at watermark");

        // New PUT arrives. Watermark advances. Next pass scans only the new entry.
        db.putItem("bkt/new", PARTITION, 300L, "req-new", 0L);
        gossipSelf();
        removed = gc.runOnce();
        assertEquals(0, removed, "new entry's key has only one live version — nothing to reap");
        assertEquals(300L, db.getGcCursor(PARTITION), "cursor advanced again");
    }

    @Test
    public void cursorCatchesStaleVersionWhenNewerArrivesAfterScan() throws Exception {
        // Verifies the holistic-key invariant: when the cursor has advanced
        // past an oplog entry that was retained as "live latest" in an
        // earlier pass, a subsequent scan won't re-visit that entry — but
        // visiting the newer entry must still reap the now-stale older
        // version from metadata.
        String key = "bkt/hist";
        db.putItem(key, PARTITION, 50L, "req-v50", 0L);

        // Simulate the cursor having already moved past 50 in some prior pass,
        // so seq=50 will NOT be re-visited by future scans.
        db.setGcCursor(PARTITION, 51L);

        // A newer version supersedes v50.
        db.putItem(key, PARTITION, 100L, "req-v100", 0L);
        // Filler to push the max seqNum past 100 so the watermark covers v100.
        db.putItem("bkt/filler", PARTITION, 110L, "req-filler", 0L);

        gossipSelf();
        int removed = gc.runOnce();

        // The scan runs from cursor=51 and stops before watermark=110, so the
        // (50, key) oplog row is never re-visited. But visiting (100, key)
        // holistically reaps the now-stale v50 from metadata anyway.
        assertEquals(1, removed,
                "visiting (100, key) reaps the now-stale v50 even though seq=50's oplog row was skipped");
        Metadata meta = db.getItem(key).orElseThrow();
        assertEquals(1, meta.versions().size());
        assertEquals(100L, meta.versions().get(0).sequenceNumber());
        assertFalse(strategy.getLog(PARTITION, 50L).isPresent(),
                "v50's oplog row is removed by the holistic cleanup");
    }

    @Test
    public void cursorSurvivesRestart() throws Exception {
        db.putItem("bkt/r", PARTITION, 10L, "req", 0L);
        db.putItem("bkt/r", PARTITION, 20L, "req2", 0L);

        gossipSelf();
        gc.runOnce();
        long persisted = db.getGcCursor(PARTITION);
        assertEquals(20L, persisted);

        // Simulate process restart by closing and reopening the strategy.
        db.close();
        strategy = new RocksDbStorageStrategy(tempDir.resolve("db").toAbsolutePath().toString());
        db = new Database(strategy);

        assertEquals(20L, db.getGcCursor(PARTITION),
                "cursor survives a restart and is read from RocksDB");
    }

    @Test
    public void liveLatestRegularVersionIsNeverDeleted() throws Exception {
        String key = "bkt/single";
        db.putItem(key, PARTITION, 1L, "req", 0L);

        // Advance partition seq with unrelated writes so watermark > 1.
        for (long s = 2; s <= 5; s++) {
            db.putItem("bkt/u-" + s, PARTITION, s, "rq-" + s, 0L);
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
