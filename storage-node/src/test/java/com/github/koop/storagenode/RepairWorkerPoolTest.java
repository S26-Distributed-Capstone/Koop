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
        if (pool != null && pool.isRunning()) {
            pool.shutdown();
        }
    }

    @Test
    void testEnqueueAndProcess() throws Exception {
        // Use a subclass to track executions
        List<RepairOperation> executed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        pool = new RepairWorkerPool(2) {
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

        pool = new RepairWorkerPool(concurrency) {
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

        // Enqueue exactly N operations
        for (int i = 0; i < concurrency; i++) {
            pool.enqueue(new RepairOperation("key-" + i, RepairOperation.RepairReason.READ_MISS));
        }

        // All N should be running concurrently
        assertTrue(inProgress.await(5, TimeUnit.SECONDS),
                "All " + concurrency + " workers should be active concurrently");

        // Release them
        release.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS), "All operations should complete");
    }

    @Test
    void testPendingCount() {
        // Use a pool that blocks on execute to keep items in the queue visible
        CountDownLatch blockLatch = new CountDownLatch(1);

        pool = new RepairWorkerPool(1) {
            @Override
            void executeRepair(RepairOperation op) {
                try {
                    blockLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        // Enqueue before starting — items stay in queue
        pool.enqueue(new RepairOperation("key1", RepairOperation.RepairReason.READ_MISS));
        pool.enqueue(new RepairOperation("key2", RepairOperation.RepairReason.READ_MISS));
        assertEquals(2, pool.pendingCount());

        // Start will pull one item for the single worker, leaving one pending
        pool.start();
        // Give the worker a moment to take from the queue
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        assertEquals(1, pool.pendingCount());

        blockLatch.countDown();
    }

    @Test
    void testGracefulShutdown() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        pool = new RepairWorkerPool(1) {
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

        // Shutdown should interrupt blocked workers
        pool.shutdown();
        assertFalse(pool.isRunning());
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
