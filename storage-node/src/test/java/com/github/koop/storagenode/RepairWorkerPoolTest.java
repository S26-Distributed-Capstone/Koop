package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RepairWorkerPoolTest {

    private RepairWorkerPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdown();
        }
    }

    // -------------------------------------------------------------------------
    // Basic enqueue / processing
    // -------------------------------------------------------------------------

    @Test
    void testEnqueueAndProcess() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        pool = new RepairWorkerPool(2, key -> false, 0) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        pool.enqueue(new RepairOperation("key2", RepairOperation.RepairReason.STARTUP_CATCHUP));
        pool.enqueue(new RepairOperation("key3", RepairOperation.RepairReason.READ_MISS));

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All 3 operations should be processed");
        assertEquals(3, executed.size());
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        int concurrency = 4;
        CountDownLatch inProgress = new CountDownLatch(concurrency);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);

        pool = new RepairWorkerPool(concurrency, key -> false, 0) {
            @Override
            void executeRepair(RepairOperation op) {
                inProgress.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                done.countDown();
            }
        };
        pool.start();

        for (int i = 0; i < concurrency; i++) {
            pool.enqueue(new RepairOperation("key-" + i, RepairOperation.RepairReason.READ_MISS));
        }

        assertTrue(inProgress.await(5, TimeUnit.SECONDS),
                "All " + concurrency + " workers should be active concurrently");

        release.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS), "All operations should complete");
    }

    // -------------------------------------------------------------------------
    // Last-writer-wins compaction
    // -------------------------------------------------------------------------

    @Test
    void testPendingCountReflectsUniqueKeys() {
        pool = new RepairWorkerPool(1);

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        pool.enqueue(new RepairOperation("key2", RepairOperation.RepairReason.READ_MISS));
        assertEquals(2, pool.pendingCount());

        // Re-enqueuing an existing key compacts — count stays at 2
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS));
        assertEquals(2, pool.pendingCount());
    }

    @Test
    void testLastWriterWinsExecutesLatestOperation() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(1, key -> false, 50) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        // Enqueue same key twice in quick succession
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Exactly one execution should fire");
        // Extra wait to confirm no second execution arrives
        Thread.sleep(150);

        assertEquals(1, executed.size(), "Only one execution for the same key");
        assertEquals(RepairOperation.RepairReason.COMMIT_MISS, executed.get(0).reason(),
                "Latest enqueued reason should win");
    }

    @Test
    void testRapidReenqueueDoesNotFireTwice() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> executions = Collections.synchronizedList(new ArrayList<>());

        pool = new RepairWorkerPool(2, key -> false, 100) {
            @Override
            void executeRepair(RepairOperation op) {
                executions.add(op.blobKey());
                latch.countDown();
            }
        };
        pool.start();

        // Enqueue the same key 10 times rapidly
        for (int i = 0; i < 10; i++) {
            pool.enqueue(new RepairOperation("hot-key", RepairOperation.RepairReason.READ_MISS));
        }
        // Count should be 1 (all compacted to 1 entry)
        assertEquals(1, pool.pendingCount());

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(200); // confirm no additional fires

        assertEquals(1, executions.size(), "Exactly one execution should fire for 10 rapid re-enqueues");
    }

    // -------------------------------------------------------------------------
    // Active write check (deferral)
    // -------------------------------------------------------------------------

    @Test
    void testActiveWriteCheckDefersRepair() throws Exception {
        AtomicBoolean writeInProgress = new AtomicBoolean(true);
        CountDownLatch executed = new CountDownLatch(1);

        pool = new RepairWorkerPool(1, key -> writeInProgress.get(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS));

        // First delay fires but write is in progress → re-enqueued, not executed
        Thread.sleep(120);
        assertEquals(1, executed.getCount(), "Should not have executed yet while write is in progress");
        assertEquals(1, pool.pendingCount(), "Should be re-enqueued while write is in progress");

        // Mark write as done → next delay fires and executes
        writeInProgress.set(false);
        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute after write completes");
    }

    @Test
    void testRepairExecutesImmediatelyWhenNoActiveWrite() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        pool = new RepairWorkerPool(1, key -> false, 50) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));

        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute when no active write");
    }

    // -------------------------------------------------------------------------
    // Map cleanup
    // -------------------------------------------------------------------------

    @Test
    void testPendingCountDropsToZeroAfterDispatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(1, key -> false, 50) {
            @Override
            void executeRepair(RepairOperation op) {
                latch.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        assertEquals(1, pool.pendingCount());

        // After dispatch, entry is removed from the map
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(0, pool.pendingCount(), "Map should be empty after dispatch");
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Test
    void testGracefulShutdown() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        pool = new RepairWorkerPool(1, key -> false, 0) {
            @Override
            void executeRepair(RepairOperation op) {
                started.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        pool.start();
        assertTrue(pool.isRunning());

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        assertTrue(started.await(5, TimeUnit.SECONDS));

        pool.shutdown();
        assertFalse(pool.isRunning());
    }

    @Test
    void testShutdownCancelsPendingScheduledTasks() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        // Long delay — task should never fire because shutdown cancels it
        pool = new RepairWorkerPool(1, key -> false, 5_000) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        assertEquals(1, pool.pendingCount());

        pool.shutdown();

        // After shutdown the scheduled task should NOT have fired
        assertFalse(executed.await(200, TimeUnit.MILLISECONDS),
                "Repair should not execute after shutdown");
    }

    @Test
    void testNullEnqueueIgnored() {
        pool = new RepairWorkerPool(1);
        pool.enqueue(null);
        assertEquals(0, pool.pendingCount());
    }

    @Test
    void testDoubleStartThrows() {
        pool = new RepairWorkerPool(1);
        pool.start();
        assertThrows(IllegalStateException.class, () -> pool.start());
    }

    @Test
    void testInvalidConcurrencyThrows() {
        assertThrows(IllegalArgumentException.class, () -> new RepairWorkerPool(0));
        assertThrows(IllegalArgumentException.class, () -> new RepairWorkerPool(-1));
    }
}
