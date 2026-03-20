package com.github.koop.queryprocessor.processor;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultipartUploadIntegrationTest {

    private List<FakeStorageNodeServer> nodes;
    private StorageWorker worker;
    private MultipartUploadManager manager;

    @BeforeAll
    void setup() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nodes.add(new FakeStorageNodeServer());
        }

        List<InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();
        worker = new StorageWorker(set, set, set);
        manager = new MultipartUploadManager(worker, new MemoryCacheClient());
    }

    @BeforeEach
    void resetNodes() {
        for (FakeStorageNodeServer node : nodes) {
            node.setEnabled(true);
        }
    }

    @AfterAll
    void teardown() throws Exception {
        worker.shutdown();
        for (FakeStorageNodeServer node : nodes) {
            node.close();
        }
    }

    @Test
    void fullLifecycle_initUploadComplete_thenGet() throws Exception {
        byte[] p1 = "hello ".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "world".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "obj-1");
        manager.uploadPart("bucket", "obj-1", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        manager.uploadPart("bucket", "obj-1", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        manager.completeMultipartUpload(
                "bucket",
                "obj-1",
                uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));

        byte[] expected = "hello world".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", "obj-1")) {
            byte[] got = in.readAllBytes();
            assertArrayEquals(expected, got);
        }
    }

    @Test
    void fullLifecycle_initUploadAbort_partsGone() throws Exception {
        byte[] payload = "temporary-part".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "obj-2");
        manager.uploadPart("bucket", "obj-2", uploadId, 1, new ByteArrayInputStream(payload), payload.length);

        String partStorageKey = MultipartUploadSession.partStorageKey("bucket", "obj-2", uploadId, 1);
        manager.abortMultipartUpload("bucket", "obj-2", uploadId);

        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", partStorageKey)) {
            got = in.readAllBytes();
        }

        assertNotEquals(payload.length, got.length, "aborted part should no longer be reconstructable");
    }

    @Test
    void uploadPart_duplicatePartNumber_throwsException() throws Exception {
        String uploadId = manager.initiateMultipartUpload("bucket", "obj-3");
        byte[] payload = "dup-part".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "obj-3", uploadId, 1, new ByteArrayInputStream(payload), payload.length);

        assertThrows(
                IllegalStateException.class,
                () -> manager.uploadPart("bucket", "obj-3", uploadId, 1,
                        new ByteArrayInputStream(payload), payload.length));
    }

    @Test
    void complete_withMissingPart_throwsException() throws Exception {
        String uploadId = manager.initiateMultipartUpload("bucket", "obj-4");
        byte[] p1 = "only-one-part".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "obj-4", uploadId, 1, new ByteArrayInputStream(p1), p1.length);

        assertThrows(
                IllegalStateException.class,
                () -> manager.completeMultipartUpload(
                        "bucket",
                        "obj-4",
                        uploadId,
                        List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2))));
    }

    @Test
    void complete_partialNodeFailure_stillSucceeds() throws Exception {
        byte[] p1 = new byte[2 * 1024 * 1024];
        byte[] p2 = new byte[2 * 1024 * 1024];
        Arrays.fill(p1, (byte) 'A');
        Arrays.fill(p2, (byte) 'B');

        String uploadId = manager.initiateMultipartUpload("bucket", "obj-5");
        manager.uploadPart("bucket", "obj-5", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        manager.uploadPart("bucket", "obj-5", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        // Erasure coding tolerates up to 3 failed nodes.
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);

        manager.completeMultipartUpload(
                "bucket",
                "obj-5",
                uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));

        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", "obj-5")) {
            byte[] got = in.readAllBytes();
            assertEqualsLengthAndPrefix(p1.length + p2.length, got);
            assertTrue(got[0] == 'A');
            assertTrue(got[p1.length] == 'B');
        }
    }

    private static void assertEqualsLengthAndPrefix(int expectedLength, byte[] actual) {
        assertTrue(actual.length == expectedLength, "assembled object length should match expected");
    }
}
