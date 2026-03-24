package com.github.koop.queryprocessor.processor;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;
import com.github.koop.storagenode.StorageNodeServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for multipart upload using real {@link StorageNodeServer} instances.
 *
 * <p>Spins up 9 real storage nodes (same pattern as {@code RealStorageNodesIT}) so that
 * the full erasure-coding path is exercised. Logic-only tests (duplicate part rejection,
 * missing part on complete, etc.) live in {@link MultipartUploadManagerTest} which uses
 * a mock {@code StorageWorker} and is faster to run.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultipartUploadIntegrationTest {

    private static final int TOTAL_NODES = 9;

    private final List<StorageNodeServer> servers = new ArrayList<>();
    private final List<Thread> serverThreads = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();

    private StorageWorker worker;
    private MultipartUploadManager manager;

    @BeforeAll
    void startCluster() throws Exception {
        List<InetSocketAddress> addrs = new ArrayList<>();

        for (int i = 0; i < TOTAL_NODES; i++) {
            int port = freePort();
            Path dir = Files.createTempDirectory("mpu-it-node-" + i + "-");
            dataDirs.add(dir);

            StorageNodeServer server = new StorageNodeServer(port, dir);
            servers.add(server);

            int nodeIdx = i;
            Thread t = Thread.ofVirtual().start(() -> server.start());
            serverThreads.add(t);

            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
            waitForReady(addr, 5_000);
            addrs.add(addr);
        }

        worker = new StorageWorker(addrs, addrs, addrs);
        manager = new MultipartUploadManager(worker, new MemoryCacheClient());
    }

    @BeforeEach
    void resetServers() {
        // All nodes are real and always up; nothing to reset between tests.
    }

    @AfterAll
    void stopCluster() throws Exception {
        for (StorageNodeServer server : servers) {
            server.stop();
        }
        for (Thread t : serverThreads) {
            t.join(1_000);
        }
        for (Path dir : dataDirs) {
            deleteRecursive(dir);
        }
        worker.shutdown();
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /**
     * Full happy-path: initiate → upload 2 parts → complete → GET returns assembled data.
     */
    @Test
    void fullLifecycle_initUploadComplete_thenGet() throws Exception {
        byte[] p1 = "hello ".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "world".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-1");
        manager.uploadPart("bucket", "mpu-obj-1", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        manager.uploadPart("bucket", "mpu-obj-1", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        manager.completeMultipartUpload(
                "bucket",
                "mpu-obj-1",
                uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));

        byte[] expected = "hello world".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", "mpu-obj-1")) {
            assertArrayEquals(expected, in.readAllBytes());
        }
    }

    /**
     * Abort: initiate → upload 1 part → abort → part key is no longer reconstructable.
     */
    @Test
    void fullLifecycle_initUploadAbort_partsGone() throws Exception {
        byte[] payload = "temporary-part".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-2");
        manager.uploadPart("bucket", "mpu-obj-2", uploadId, 1, new ByteArrayInputStream(payload), payload.length);

        String partStorageKey = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-2", uploadId, 1);
        manager.abortMultipartUpload("bucket", "mpu-obj-2", uploadId);

        // After abort the part shards are deleted; reading the part key should not
        // return the original payload (erasure reconstruction will fail or return empty).
        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", partStorageKey)) {
            got = in.readAllBytes();
        } catch (Exception ignored) {
            got = new byte[0];
        }

        assertNotEquals(payload.length, got.length,
                "aborted part should no longer be reconstructable");
    }

    /**
     * Erasure tolerance: complete succeeds even with 3 nodes down during assembly.
     */
    @Test
    void complete_partialNodeFailure_stillSucceeds() throws Exception {
        byte[] p1 = new byte[2 * 1024 * 1024];
        byte[] p2 = new byte[2 * 1024 * 1024];
        Arrays.fill(p1, (byte) 'A');
        Arrays.fill(p2, (byte) 'B');

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-3");
        manager.uploadPart("bucket", "mpu-obj-3", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        manager.uploadPart("bucket", "mpu-obj-3", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        // Stop 3 nodes — erasure coding (6 data + 3 parity) should still reconstruct.
        servers.get(0).stop();
        servers.get(1).stop();
        servers.get(2).stop();

        try {
            manager.completeMultipartUpload(
                    "bucket",
                    "mpu-obj-3",
                    uploadId,
                    List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));

            try (InputStream in = worker.get(UUID.randomUUID(), "bucket", "mpu-obj-3")) {
                byte[] got = in.readAllBytes();
                assertTrue(got.length == p1.length + p2.length,
                        "assembled object length should match expected");
                assertTrue(got[0] == 'A', "first byte should be from part 1");
                assertTrue(got[p1.length] == 'B', "first byte of part 2 should be 'B'");
            }
        } finally {
            // Restart the stopped nodes so teardown can proceed cleanly.
            for (int i = 0; i < 3; i++) {
                int port = servers.get(i).port();
                Path dir = dataDirs.get(i);
                StorageNodeServer replacement = new StorageNodeServer(port, dir);
                servers.set(i, replacement);
                Thread.ofVirtual().start(replacement::start);
                try {
                    waitForReady(new InetSocketAddress("localhost", port), 10_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting for restarted storage node to become ready on port " + port, e);
                }
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static void waitForReady(InetSocketAddress addr, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        HttpClient client = HttpClient.newHttpClient();
        URI healthUri = URI.create(
                "http://" + addr.getHostString() + ":" + addr.getPort() + "/health");

        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest req = HttpRequest.newBuilder().uri(healthUri).GET().build();
                HttpResponse<String> resp =
                        client.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) return;
            } catch (IOException ignored) {
                // not ready yet
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Storage node did not become ready: " + addr);
    }

    private static void deleteRecursive(Path root) throws IOException {
        if (!Files.exists(root)) return;
        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); }
                    catch (IOException ignored) {}
                });
    }
}
