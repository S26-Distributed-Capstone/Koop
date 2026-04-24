package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

        pool = new RepairWorkerPool(new WriteTracker(), 0L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        pool.enqueue(new RepairOperation("key2", RepairOperation.RepairReason.STARTUP_CATCHUP, 1L));
        pool.enqueue(new RepairOperation("key3", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All 3 operations should be processed");
        assertEquals(3, executed.size());
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        int numOps = 4;
        CountDownLatch inProgress = new CountDownLatch(numOps);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numOps);

        pool = new RepairWorkerPool(new WriteTracker(), 0L) {
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

        for (int i = 0; i < numOps; i++) {
            pool.enqueue(new RepairOperation("key-" + i, RepairOperation.RepairReason.READ_MISS, (long) i));
        }

        assertTrue(inProgress.await(5, TimeUnit.SECONDS),
                "All " + numOps + " virtual-thread workers should be active concurrently");

        release.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS), "All operations should complete");
    }

    // -------------------------------------------------------------------------
    // Last-writer-wins compaction
    // -------------------------------------------------------------------------

    @Test
    void testPendingCountReflectsUniqueKeys() {
        pool = new RepairWorkerPool(new WriteTracker());

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        pool.enqueue(new RepairOperation("key2", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        assertEquals(2, pool.pendingCount());

        // Re-enqueuing key1 with a higher seqOffset compacts — count stays at 2
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, Long.MAX_VALUE));
        assertEquals(2, pool.pendingCount());
    }

    @Test
    void testLastWriterWinsExecutesLatestOperation() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        // Enqueue same key twice: second has higher seqOffset so it wins
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, 1L));
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, 2L));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Exactly one execution should fire");
        Thread.sleep(150); // confirm no second execution arrives

        assertEquals(1, executed.size(), "Only one execution for the same key");
        assertEquals(RepairOperation.RepairReason.COMMIT_MISS, executed.get(0).reason(),
                "Higher seqOffset operation should win");
    }

    @Test
    void testRapidReenqueueDoesNotFireTwice() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> executions = Collections.synchronizedList(new ArrayList<>());

        pool = new RepairWorkerPool(new WriteTracker(), 100L) {
            @Override
            void executeRepair(RepairOperation op) {
                executions.add(op.blobKey());
                latch.countDown();
            }
        };
        pool.start();

        // Enqueue same key 10 times with ascending seqOffsets
        for (int i = 0; i < 10; i++) {
            pool.enqueue(new RepairOperation("hot-key", RepairOperation.RepairReason.READ_MISS, (long) i));
        }
        assertEquals(1, pool.pendingCount(), "All enqueues for same key should compact to 1");

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(200); // confirm no additional fires

        assertEquals(1, executions.size(), "Exactly one execution for 10 rapid re-enqueues");
    }

    // -------------------------------------------------------------------------
    // Idempotency: seqOffset-based discard
    // -------------------------------------------------------------------------

    @Test
    void testOlderSeqOffsetDiscarded() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        // Higher seqOffset enqueued first, then lower — lower should be discarded
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, 10L));
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, 5L));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size(), "Only one execution should fire");
        assertEquals(10L, executed.get(0).seqOffset(), "Higher seqOffset should execute");
        assertEquals(RepairOperation.RepairReason.COMMIT_MISS, executed.get(0).reason());
    }

    @Test
    void testNewerSeqOffsetOverwritesPending() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        // Lower seqOffset enqueued first, then higher — higher should overwrite
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, 5L));
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, 10L));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size(), "Only one execution should fire");
        assertEquals(10L, executed.get(0).seqOffset(), "Newer seqOffset should win");
        assertEquals(RepairOperation.RepairReason.COMMIT_MISS, executed.get(0).reason());
    }

    @Test
    void testEqualSeqOffsetDoesNotOverwrite() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.add(op);
                latch.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, 7L));
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, 7L));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size());
        assertEquals(RepairOperation.RepairReason.COMMIT_MISS, executed.get(0).reason(),
                "Equal seqOffset should not overwrite — first enqueue wins");
    }

    // -------------------------------------------------------------------------
    // Active write check (deferral)
    // -------------------------------------------------------------------------

    @Test
    void testActiveWriteCheckDefersRepair() throws Exception {
        WriteTracker writeTracker = new WriteTracker();
        CountDownLatch executed = new CountDownLatch(1);

        pool = new RepairWorkerPool(writeTracker, 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        // Mark write as in progress before the delay fires
        writeTracker.begin("key1");
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.COMMIT_MISS, 1L));

        // Delay fires but write is still in progress → re-enqueued, not executed
        Thread.sleep(120);
        assertEquals(1, executed.getCount(), "Should not have executed while write is in progress");
        assertEquals(1, pool.pendingCount(), "Should be re-enqueued while write is in progress");

        // Release write → next delay fires and executes
        writeTracker.end("key1");
        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute after write completes");
    }

    @Test
    void testRepairExecutesImmediatelyWhenNoActiveWrite() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));

        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute when no active write");
    }

    // -------------------------------------------------------------------------
    // Map cleanup
    // -------------------------------------------------------------------------

    @Test
    void testPendingCountDropsToZeroAfterDispatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        pool = new RepairWorkerPool(new WriteTracker(), 50L) {
            @Override
            void executeRepair(RepairOperation op) {
                latch.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        assertEquals(1, pool.pendingCount());

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

        pool = new RepairWorkerPool(new WriteTracker(), 0L) {
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

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        assertTrue(started.await(5, TimeUnit.SECONDS));

        pool.shutdown();
        assertFalse(pool.isRunning());
    }

    @Test
    void testShutdownCancelsPendingScheduledTasks() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        // Long delay — task should never fire because shutdown cancels it
        pool = new RepairWorkerPool(new WriteTracker(), 5_000L) {
            @Override
            void executeRepair(RepairOperation op) {
                executed.countDown();
            }
        };
        pool.start();

        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS, Long.MAX_VALUE));
        assertEquals(1, pool.pendingCount());

        pool.shutdown();

        assertFalse(executed.await(200, TimeUnit.MILLISECONDS),
                "Repair should not execute after shutdown");
    }

    @Test
    void testNullEnqueueIgnored() {
        pool = new RepairWorkerPool(new WriteTracker());
        pool.enqueue(null);
        assertEquals(0, pool.pendingCount());
    }

    @Test
    void testDoubleStartThrows() {
        pool = new RepairWorkerPool(new WriteTracker());
        pool.start();
        assertThrows(IllegalStateException.class, () -> pool.start());
    }
}
