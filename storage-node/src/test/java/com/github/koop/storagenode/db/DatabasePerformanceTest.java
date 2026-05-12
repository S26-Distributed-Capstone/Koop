package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

@Tag("performance")
class DatabasePerformanceTest {

    @TempDir
    static Path tempDir;

    private static RocksDbStorageStrategy strategy;
    private static Database database;

    @BeforeAll
    static void setUpAll() throws Exception {
        strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
        database = new Database(strategy);
    }

    @AfterAll
    static void tearDownAll() throws Exception {
        if (database != null) {
            database.close();
        }
    }

    @Test
    void measureDatabaseThroughput() throws Exception {
        int numOperations = 50_000;
        int partition = 1;

        long writeStartNanos = System.nanoTime();

        for (long i = 0; i < numOperations; i++) {
            String key = "db-perf-data-" + i;
            String requestId = UUID.randomUUID().toString();

            database.putUncommittedWrite(requestId, System.currentTimeMillis());
            database.putItem(key, partition, i, requestId, 1024L);
        }

        long writeEndNanos = System.nanoTime();
        double writeDurationMs = (writeEndNanos - writeStartNanos) / 1_000_000.0;
        double writesPerSecond = (numOperations / writeDurationMs) * 1000.0;

        System.out.printf("[Performance] Database Writes: %d operations in %.2f ms (%.2f ops/sec)%n",
                numOperations, writeDurationMs, writesPerSecond);

        long readStartNanos = System.nanoTime();

        for (long i = 0; i < numOperations; i++) {
            String key = "db-perf-data-" + i;
            database.getItem(key);
        }

        long readEndNanos = System.nanoTime();
        double readDurationMs = (readEndNanos - readStartNanos) / 1_000_000.0;
        double readsPerSecond = (numOperations / readDurationMs) * 1000.0;

        System.out.printf("[Performance] Database Reads:  %d operations in %.2f ms (%.2f ops/sec)%n",
                numOperations, readDurationMs, readsPerSecond);
    }
}