package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

@Tag("performance")
class RocksDbPerformanceTest {

    @TempDir
    static Path tempDir;

    private static RocksDbStorageStrategy strategy;

    @BeforeAll
    static void setUpAll() throws Exception {
        strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    static void tearDownAll() throws Exception {
        if (strategy != null) {
            strategy.close();
        }
    }

    @Test
    void measureSequentialWriteAndReadThroughput() throws Exception {
        int numOperations = 50_000;
        int partition = 1;

        long writeStartNanos = System.nanoTime();

        for (long i = 0; i < numOperations; i++) {
            String key = "perf-data-" + i;
            try (StorageTransaction txn = strategy.beginTransaction()) {
                txn.putLog(new OpLog(partition, i, key, Operation.PUT));
                txn.putMetadata(new Metadata(key, partition, List.of(new RegularFileVersion(i, "/blob-" + i, true, 1024L))));
                txn.commit();
            }
        }

        long writeEndNanos = System.nanoTime();
        double writeDurationMs = (writeEndNanos - writeStartNanos) / 1_000_000.0;
        double writesPerSecond = (numOperations / writeDurationMs) * 1000.0;

        System.out.printf("[Performance] Writes: %d operations in %.2f ms (%.2f ops/sec)%n",
                numOperations, writeDurationMs, writesPerSecond);

        long readStartNanos = System.nanoTime();

        for (long i = 0; i < numOperations; i++) {
            String key = "perf-data-" + i;
            try (StorageTransaction txn = strategy.beginTransaction()) {
                txn.getMetadata(key);
            }
        }

        long readEndNanos = System.nanoTime();
        double readDurationMs = (readEndNanos - readStartNanos) / 1_000_000.0;
        double readsPerSecond = (numOperations / readDurationMs) * 1000.0;

        System.out.printf("[Performance] Reads:  %d operations in %.2f ms (%.2f ops/sec)%n",
                numOperations, readDurationMs, readsPerSecond);
    }
}