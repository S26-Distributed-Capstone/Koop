package koop;

import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.storagenode.StorageNodeServer;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.SecureRandom;
import java.time.LocalTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RealStorageNodesIT {

    private static final int TOTAL_NODES = 9;
    private static final int DATA_SIZE = 15 * 1024 * 1024; // 15MB

    private final List<StorageNodeServer> servers = new ArrayList<>();
    private final List<Thread> serverThreads = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private List<InetSocketAddress> addrs;

    private StorageWorker worker;

    // -------------------------------------------------------
    // Logging helper
    // -------------------------------------------------------
    private static void log(String msg) {
        System.out.printf("[%s] %s%n",
                LocalTime.now().withNano(0),
                msg);
    }

    // -------------------------------------------------------
    // Setup cluster
    // -------------------------------------------------------
    @BeforeEach
    void startRealNodes() throws Exception {

        log("=== STARTING STORAGE CLUSTER ===");

        addrs = new ArrayList<>();

        for (int i = 0; i < TOTAL_NODES; i++) {

            int port = freePort();
            Path dir = Files.createTempDirectory("storagenode-" + i + "-");

            log("Starting node " + i + " on port " + port);

            StorageNodeServer server =
                    new StorageNodeServer(port, dir);

            servers.add(server);
            dataDirs.add(dir);

            int nodeId = i;

            Thread t = Thread.ofVirtual().start(() -> {
                log("[NODE " + nodeId + "] accept loop starting");
                server.start();
            });

            serverThreads.add(t);

            InetSocketAddress addr =
                    new InetSocketAddress("127.0.0.1", port);

            waitForPortOpen(addr, 3000);

            log("[NODE " + nodeId + "] READY");

            addrs.add(addr);
        }

        worker = new StorageWorker(addrs, addrs, addrs);

        log("=== CLUSTER READY ===");
    }

    // -------------------------------------------------------
    // Shutdown cluster
    // -------------------------------------------------------
    @AfterEach
    void stopRealNodes() throws Exception {

        log("=== STOPPING CLUSTER ===");

        for (int i = 0; i < servers.size(); i++) {
            log("Stopping node " + i);
            servers.get(i).stop();
        }

        for (Thread t : serverThreads)
            t.join(1000);

        for (Path d : dataDirs)
            deleteRecursive(d);

        servers.clear();
        serverThreads.clear();
        dataDirs.clear();

        log("=== CLUSTER STOPPED ===");
    }

    // -------------------------------------------------------
    // MAIN ROUNDTRIP TEST
    // -------------------------------------------------------
    @Test
    void put_get_delete_roundTrip_realServers() throws Exception {

        log("Generating random test data...");
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        UUID req = UUID.randomUUID();

        log("[WORKER] PUT starting...");
        boolean putOk =
                worker.put(req, "b", "realA",
                        data.length,
                        new ByteArrayInputStream(data));

        log("[WORKER] PUT result = " + putOk);
        assertTrue(putOk);

        log("[WORKER] GET starting...");
        try (InputStream in =
                     worker.get(UUID.randomUUID(), "b", "realA")) {

            byte[] got = in.readAllBytes();

            log("[WORKER] GET received " + got.length + " bytes");

            assertArrayEquals(data, got);
        }

        log("[WORKER] DELETE starting...");
        boolean delOk =
                worker.delete(UUID.randomUUID(), "b", "realA");

        log("[WORKER] DELETE result = " + delOk);
        assertTrue(delOk);

        log("Round-trip test completed successfully.");
    }

    // -------------------------------------------------------
    // ERASURE TESTS (real servers)
    // -------------------------------------------------------

    /**
     * M=3 parity shards, so losing any 3 shard servers should still reconstruct.
     */
    @Test
    void get_tolerates_three_node_failures_realServers() throws Exception {

        log("Generating random test data for erasure tolerance test...");
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        String key = "erasureB";
        UUID putReq = UUID.randomUUID();

        log("[WORKER] PUT starting (erasureB)...");
        boolean putOk = worker.put(putReq, "b", key, data.length, new ByteArrayInputStream(data));
        log("[WORKER] PUT result = " + putOk);
        assertTrue(putOk, "PUT should succeed before failures");

        // Stop 3 nodes (simulate 3 shard failures)
        log("Simulating 3 node failures (should still reconstruct)...");
        stopNodes(0, 1, 2);

        log("[WORKER] GET starting with 3 nodes down...");
        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
            got = in.readAllBytes();
        }

        log("[WORKER] GET received " + got.length + " bytes with 3 nodes down");
        assertArrayEquals(data, got, "Should reconstruct full object with 3 nodes down");
        log("Erasure tolerance test (3 failures) completed successfully.");
    }

    /**
     * With K=6, M=3, losing 4 shards should be unrecoverable. Depending on the worker,
     * this might throw during read, or might return a shorter/corrupt stream.
     * We accept either behavior, but it must NOT return the full correct object.
     */
    @Test
    void get_fails_with_four_node_failures_realServers() throws Exception {

        log("Generating random test data for erasure failure test...");
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        String key = "erasureC";
        UUID putReq = UUID.randomUUID();

        log("[WORKER] PUT starting (erasureC)...");
        boolean putOk = worker.put(putReq, "b", key, data.length, new ByteArrayInputStream(data));
        log("[WORKER] PUT result = " + putOk);
        assertTrue(putOk, "PUT should succeed before failures");

        // Stop 4 nodes (simulate 4 shard failures)
        log("Simulating 4 node failures (should NOT reconstruct)...");
        stopNodes(0, 1, 2, 3);

        log("[WORKER] GET starting with 4 nodes down (expect failure)...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {

            byte[] got = in.readAllBytes();
            log("[WORKER] GET returned " + got.length + " bytes with 4 nodes down");

            // Must not match the original fully
            if (Arrays.equals(data, got)) {
                fail("Expected reconstruction to fail with 4 nodes down, but got full correct data");
            }

            // Usually this will be shorter, so this is a helpful additional check:
            assertNotEquals(data.length, got.length,
                    "Expected missing/corrupt data length with 4 nodes down");
        } catch (Exception e) {
            // Also acceptable: the read/reconstruct throws
            log("[WORKER] GET failed with exception as expected: " + e);
        }

        log("Erasure failure test (4 failures) completed.");
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    private void stopNodes(int... idxs) {
        for (int idx : idxs) {
            if (idx < 0 || idx >= servers.size()) {
                throw new IllegalArgumentException("Bad node index: " + idx);
            }
            log("Stopping node " + idx + " to simulate failure");
            servers.get(idx).stop();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static void waitForPortOpen(
            InetSocketAddress addr,
            long timeoutMs) throws InterruptedException {

        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(addr, 200);
                return;
            } catch (IOException ignored) {
                Thread.sleep(50);
            }
        }

        fail("Server did not open port: " + addr);
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
