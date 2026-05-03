package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.github.koop.storagenode.db.RocksDbStorageStrategy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RepairWorkerPoolTest {

    private RepairWorkerPool pool;
    private RocksDbRepairQueue queue;

    @TempDir
    Path tempDir;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdown();
        }
    }

    private RocksDbRepairQueue createQueue() throws Exception {
        RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(
                tempDir.resolve("db-" + System.nanoTime()).toAbsolutePath().toString());
        queue = new RocksDbRepairQueue(strategy);
        return queue;
    }

    // -------------------------------------------------------------------------
    // Basic enqueue / processing
    // -------------------------------------------------------------------------

    @Test
    void testEnqueueAndProcess() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 1L, op -> {
            executed.add(op);
            latch.countDown();
        });
        pool.start();

        queue.enqueue(new RepairOperation("key1", 1L, null));
        queue.enqueue(new RepairOperation("key2", 2L, null));
        queue.enqueue(new RepairOperation("key3", 3L, null));

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All 3 operations should be processed");
        assertEquals(3, executed.size());
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        int numOps = 4;
        CountDownLatch inProgress = new CountDownLatch(numOps);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numOps);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 1L, op -> {
            inProgress.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            done.countDown();
        });
        pool.start();

        for (int i = 0; i < numOps; i++) {
            queue.enqueue(new RepairOperation("key-" + i, (long) i, null));
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
    void testPendingCountReflectsUniqueKeys() throws Exception {
        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 60_000L, op -> {});

        queue.enqueue(new RepairOperation("key1", 1L, null));
        queue.enqueue(new RepairOperation("key2", 2L, null));
        assertEquals(2, pool.pendingCount());

        // Re-enqueuing key1 with a higher seqOffset compacts — count stays at 2
        queue.enqueue(new RepairOperation("key1", 3L, null));
        assertEquals(2, pool.pendingCount());
    }

    @Test
    void testLastWriterWinsExecutesLatestOperation() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> {
            executed.add(op);
            latch.countDown();
        });
        pool.start();

        // Enqueue same key twice: second has higher seqOffset so it wins
        queue.enqueue(new RepairOperation("key1", 1L, null));
        queue.enqueue(new RepairOperation("key1", 2L, null));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Exactly one execution should fire");
        Thread.sleep(150); // confirm no second execution arrives

        assertEquals(1, executed.size(), "Only one execution for the same key");
        assertEquals(2L, executed.get(0).seqOffset(), "Higher seqOffset operation should win");
    }

    @Test
    void testRapidReenqueueDoesNotFireTwice() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> executions = Collections.synchronizedList(new ArrayList<>());

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 100L, op -> {
            executions.add(op.blobKey());
            latch.countDown();
        });
        pool.start();

        // Enqueue same key 10 times with ascending seqOffsets
        for (int i = 0; i < 10; i++) {
            queue.enqueue(new RepairOperation("hot-key", (long) i, null));
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

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> {
            executed.add(op);
            latch.countDown();
        });
        pool.start();

        // Higher seqOffset enqueued first, then lower — lower should be discarded
        queue.enqueue(new RepairOperation("key1", 10L, null));
        queue.enqueue(new RepairOperation("key1", 5L, null));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size(), "Only one execution should fire");
        assertEquals(10L, executed.get(0).seqOffset(), "Higher seqOffset should execute");
    }

    @Test
    void testNewerSeqOffsetOverwritesPending() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> {
            executed.add(op);
            latch.countDown();
        });
        pool.start();

        // Lower seqOffset enqueued first, then higher — higher should overwrite
        queue.enqueue(new RepairOperation("key1", 5L, null));
        queue.enqueue(new RepairOperation("key1", 10L, null));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size(), "Only one execution should fire");
        assertEquals(10L, executed.get(0).seqOffset(), "Newer seqOffset should win");
    }

    @Test
    void testEqualSeqOffsetDoesNotOverwrite() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> {
            executed.add(op);
            latch.countDown();
        });
        pool.start();

        queue.enqueue(new RepairOperation("key1", 7L, null));
        queue.enqueue(new RepairOperation("key1", 7L, null));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, executed.size());
        assertEquals(7L, executed.get(0).seqOffset(),
                "Equal seqOffset should not overwrite — first enqueue wins");
    }

    // -------------------------------------------------------------------------
    // Active write check (deferral)
    // -------------------------------------------------------------------------

    @Test
    void testActiveWriteCheckDefersRepair() throws Exception {
        WriteTracker writeTracker = new WriteTracker();
        CountDownLatch executed = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, writeTracker, 50L, op -> executed.countDown());
        pool.start();

        // Mark write as in progress before the poll fires
        writeTracker.begin("key1");
        queue.enqueue(new RepairOperation("key1", 1L, null));

        // Poll fires but write is still in progress → deferred
        Thread.sleep(120);
        assertEquals(1, executed.getCount(), "Should not have executed while write is in progress");
        assertEquals(1, pool.pendingCount(), "Should remain pending while write is in progress");

        // Release write → next poll fires and executes
        writeTracker.end("key1");
        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute after write completes");
    }

    @Test
    void testRepairExecutesImmediatelyWhenNoActiveWrite() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> executed.countDown());
        pool.start();

        queue.enqueue(new RepairOperation("key1", 1L, null));

        assertTrue(executed.await(2, TimeUnit.SECONDS), "Should execute when no active write");
    }

    // -------------------------------------------------------------------------
    // Map cleanup
    // -------------------------------------------------------------------------

    @Test
    void testPendingCountDropsToZeroAfterDispatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 50L, op -> latch.countDown());
        pool.start();

        queue.enqueue(new RepairOperation("key1", 1L, null));
        assertEquals(1, pool.pendingCount());

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        Thread.sleep(100); // allow remove() to complete
        assertEquals(0, pool.pendingCount(), "Map should be empty after dispatch");
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Test
    void testGracefulShutdown() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 1L, op -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        pool.start();
        assertTrue(pool.isRunning());

        queue.enqueue(new RepairOperation("key1", 1L, null));
        assertTrue(started.await(5, TimeUnit.SECONDS));

        pool.shutdown();
        assertFalse(pool.isRunning());
    }

    @Test
    void testShutdownCancelsPendingScheduledTasks() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);

        // Long delay — task should never fire because shutdown cancels it
        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 5_000L, op -> executed.countDown());
        pool.start();

        queue.enqueue(new RepairOperation("key1", 1L, null));
        assertEquals(1, pool.pendingCount());

        pool.shutdown();

        assertFalse(executed.await(200, TimeUnit.MILLISECONDS),
                "Repair should not execute after shutdown");
    }

    @Test
    void testNullEnqueueIgnored() throws Exception {
        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 60_000L, op -> {});
        queue.enqueue(null);
        assertEquals(0, pool.pendingCount());
    }

    @Test
    void testDoubleStartThrows() throws Exception {
        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 60_000L, op -> {});
        pool.start();
        assertThrows(IllegalStateException.class, () -> pool.start());
    }

    // -------------------------------------------------------------------------
    // Strategy delegation
    // -------------------------------------------------------------------------

    @Test
    void testExecuteRepairDelegatesToStrategy() throws Exception {
        List<RepairOperation> strategyReceived = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 1L, op -> {
            strategyReceived.add(op);
            latch.countDown();
        });
        pool.start();

        RepairOperation op = new RepairOperation("test-key", 42L, null);
        queue.enqueue(op);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, strategyReceived.size());
        assertEquals("test-key", strategyReceived.get(0).blobKey());
        assertEquals(42L, strategyReceived.get(0).seqOffset());
    }

    @Test
    void testNullStrategyDoesNotThrow() throws Exception {
        queue = createQueue();
        pool = new RepairWorkerPool(queue, new WriteTracker(), 1L, null);
        pool.start();

        queue.enqueue(new RepairOperation("key1", 1L, null));
        Thread.sleep(200); // let poll fire
        // Should not throw — just logs a warning
        assertEquals(0, pool.pendingCount());
    }

    // -------------------------------------------------------------------------
    // Crash recovery: persistent queue survives pool restart cycles
    // -------------------------------------------------------------------------

    @Test
    void testPersistentQueueSurvivesMultipleRestartCycles() throws Exception {
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);
        String dbPath = tempDir.resolve("restart-db").toAbsolutePath().toString();

        // --- Cycle 1: enqueue while pool is NOT running, then shut down ---
        RocksDbStorageStrategy strategy1 = new RocksDbStorageStrategy(dbPath);
        RocksDbRepairQueue queue1 = new RocksDbRepairQueue(strategy1);
        // Pool never started — simulate node being offline while repairs arrive
        queue1.enqueue(new RepairOperation("key-a", 10L, "req-a"));
        queue1.enqueue(new RepairOperation("key-b", 20L, "req-b"));
        assertEquals(2, queue1.size());
        strategy1.close();

        // --- Cycle 2: restart, pool runs but shuts down before completing ---
        RocksDbStorageStrategy strategy2 = new RocksDbStorageStrategy(dbPath);
        RocksDbRepairQueue queue2 = new RocksDbRepairQueue(strategy2);
        assertEquals(2, queue2.size(), "Both operations should survive restart");

        // Start pool with a strategy that blocks until interrupted — simulates crash mid-repair
        CountDownLatch blockRepair = new CountDownLatch(1);
        RepairWorkerPool pool2 = new RepairWorkerPool(queue2, new WriteTracker(), 1L, op -> {
            try {
                blockRepair.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("simulated crash", e);
            }
        });
        pool2.start();
        Thread.sleep(100); // let poll dispatch
        // Shut down before repairs complete — simulates crash
        pool2.shutdown();
        strategy2.close();

        // --- Cycle 3: final restart — operations should still be in the queue ---
        RocksDbStorageStrategy strategy3 = new RocksDbStorageStrategy(dbPath);
        RocksDbRepairQueue queue3 = new RocksDbRepairQueue(strategy3);
        assertEquals(2, queue3.size(), "Operations should survive a second restart");

        RepairWorkerPool pool3 = new RepairWorkerPool(queue3, new WriteTracker(), 1L, op -> {
            executed.add(op);
            if (executed.size() == 2) latch.countDown();
        });
        pool3.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Both operations should eventually execute");
        assertEquals(2, executed.size());
        assertTrue(executed.stream().anyMatch(op -> op.blobKey().equals("key-a") && op.seqOffset() == 10L));
        assertTrue(executed.stream().anyMatch(op -> op.blobKey().equals("key-b") && op.seqOffset() == 20L));

        // Queue should be drained after successful processing
        Thread.sleep(100);
        assertEquals(0, queue3.size(), "Queue should be empty after successful repairs");

        pool3.shutdown();
        strategy3.close();
    }
}
