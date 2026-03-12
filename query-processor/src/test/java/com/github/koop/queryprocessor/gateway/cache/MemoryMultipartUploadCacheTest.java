package com.github.koop.queryprocessor.gateway.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class MemoryMultipartUploadCacheTest {

    private MemoryMultipartUploadCache cache;

    @BeforeEach
    void setUp() {
        cache = new MemoryMultipartUploadCache();
    }

    @Test
    void createUpload_storesUpload() {
        cache.createUpload("id-1", "my-bucket", "my-key");

        assertTrue(cache.uploadExists("id-1"));
    }

    @Test
    void uploadExists_returnsFalse_forUnknownId() {
        assertFalse(cache.uploadExists("unknown-id"));
    }

    @Test
    void addPart_appendsToUpload() {
        cache.createUpload("id-2", "bucket", "key");
        cache.addPart("id-2", 1, "etag-abc");

        List<MultipartUploadCache.UploadedPart> parts = cache.getParts("id-2");
        assertEquals(1, parts.size());
        assertEquals(new MultipartUploadCache.UploadedPart(1, "etag-abc"), parts.get(0));
    }

    @Test
    void addPart_appendsMultipleParts() {
        cache.createUpload("id-3", "bucket", "key");
        cache.addPart("id-3", 1, "etag-1");
        cache.addPart("id-3", 2, "etag-2");
        cache.addPart("id-3", 3, "etag-3");

        List<MultipartUploadCache.UploadedPart> parts = cache.getParts("id-3");
        assertEquals(3, parts.size());
        assertEquals(new MultipartUploadCache.UploadedPart(1, "etag-1"), parts.get(0));
        assertEquals(new MultipartUploadCache.UploadedPart(2, "etag-2"), parts.get(1));
        assertEquals(new MultipartUploadCache.UploadedPart(3, "etag-3"), parts.get(2));
    }

    @Test
    void getParts_returnsEmptyList_whenNoSuchUpload() {
        List<MultipartUploadCache.UploadedPart> parts = cache.getParts("does-not-exist");

        assertNotNull(parts);
        assertTrue(parts.isEmpty());
    }

    @Test
    void deleteUpload_removesEntry() {
        cache.createUpload("id-4", "bucket", "key");
        cache.addPart("id-4", 1, "etag-x");

        cache.deleteUpload("id-4");

        assertFalse(cache.uploadExists("id-4"));
        assertTrue(cache.getParts("id-4").isEmpty());
    }

    @Test
    void deleteUpload_isNoOp_forUnknownId() {
        // Should not throw
        assertDoesNotThrow(() -> cache.deleteUpload("ghost-id"));
    }

    @Test
    void addPart_throwsForUnknownUpload() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.addPart("no-such-upload", 1, "etag"));
    }

    @Test
    void concurrentAddParts_allPartsRecorded() throws InterruptedException {
        cache.createUpload("concurrent-id", "bucket", "key");

        int threadCount = 20;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 1; i <= threadCount; i++) {
            final int partNum = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                cache.addPart("concurrent-id", partNum, "etag-" + partNum);
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

        List<MultipartUploadCache.UploadedPart> parts = cache.getParts("concurrent-id");
        assertEquals(threadCount, parts.size());

        // Verify all part numbers 1..threadCount are present
        List<Integer> partNumbers = new ArrayList<>();
        for (var part : parts) {
            partNumbers.add(part.partNumber());
        }
        for (int i = 1; i <= threadCount; i++) {
            assertTrue(partNumbers.contains(i), "Missing part " + i);
        }
    }
}
