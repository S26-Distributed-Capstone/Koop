package koop;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration.PartitionSpread;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.queryprocessor.processor.CommitCoordinator;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.storagenode.StorageNodeServerV2;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;
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
    private static final int DATA_SIZE = 60 * 1024 * 1024; // 75MB

    private final List<StorageNodeServerV2> servers = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private final List<Database> databases = new ArrayList<>();
    private List<InetSocketAddress> addrs;

    // Shared infrastructure for V2
    private MemoryFetcher sharedFetcher;
    private MemoryPubSub sharedPubSub;
    private MetadataClient sharedMetadataClient;
    private PubSubClient sharedPubSubClient;
    private CommitCoordinator commitCoordinator;
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
        sharedFetcher = new MemoryFetcher();
        sharedPubSub = new MemoryPubSub();

        // 1. Initialize shared control plane
        sharedPubSubClient = new PubSubClient(sharedPubSub);
        sharedPubSubClient.start();

        sharedMetadataClient = new MetadataClient(sharedFetcher);
        sharedMetadataClient.start();

        // 2. Init QP Commit Coordinator and Storage Worker
        commitCoordinator = new CommitCoordinator(sharedPubSubClient, 0, 10);
        worker = new StorageWorker(sharedMetadataClient, commitCoordinator);

        // 3. Start Storage Node Servers (V2)
        for (int i = 0; i < TOTAL_NODES; i++) {

            int port = freePort();
            Path dir = Files.createTempDirectory("storagenode-" + i + "-");

            log("Starting node " + i + " on port " + port);

            // Each node gets its own Database instance but shares the memory-backed control planes
            Database db = new Database(new RocksDbStorageStrategy(dir.resolve("db").toString()));
            StorageNodeServerV2 server =
                    new StorageNodeServerV2(port, "127.0.0.1", db, dir.resolve("data"), sharedMetadataClient, sharedPubSubClient);

            servers.add(server);
            dataDirs.add(dir);
            databases.add(db);

            log("[NODE " + i + "] server starting");
            
            // Start server directly (non-blocking in Javalin)
            server.start();

            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);

            log("[NODE " + i + "] READY");

            addrs.add(addr);
        }

        // 4. Populate cluster configuration so components can discover each other
        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        ErasureSet es = new ErasureSet();
        es.setNumber(1);
        es.setN(9);
        es.setK(6);
        es.setWriteQuorum(7);
        
        List<Machine> machines = new ArrayList<>();
        for (InetSocketAddress addr : addrs) {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            machines.add(m);
        }
        es.setMachines(machines);
        esConfig.setErasureSets(List.of(es));

        PartitionSpreadConfiguration psConfig = new PartitionSpreadConfiguration();
        PartitionSpread ps = new PartitionSpread();
        ps.setErasureSet(1);
        List<Integer> parts = new ArrayList<>();
        for(int i = 0; i < 3; i++) parts.add(i); // Assign all partitions to this set
        ps.setPartitions(parts);
        psConfig.setPartitionSpread(List.of(ps));

        // Push updates to the shared MemoryFetcher
        sharedFetcher.update(esConfig);
        sharedFetcher.update(psConfig);

        // Sleep briefly to let the pubsub/metadata configuration apply locally across the instances
        Thread.sleep(1000);

        log("=== CLUSTER READY ===");
    }

    // -------------------------------------------------------
    // Shutdown cluster
    // -------------------------------------------------------
    @AfterEach
    void stopRealNodes() throws Exception {

        log("=== STOPPING CLUSTER ===");

        if (worker != null) worker.shutdown();
        if (sharedMetadataClient != null) sharedMetadataClient.close();
        if (sharedPubSubClient != null) sharedPubSubClient.close();

        for (int i = 0; i < servers.size(); i++) {
            log("Stopping node " + i);
            servers.get(i).stop();
        }

        for (Database db : databases) {
            try { 
                db.close(); 
            } catch (Exception ignored) {}
        }

        for (Path d : dataDirs)
            deleteRecursive(d);

        servers.clear();
        databases.clear();
        dataDirs.clear();

        log("=== CLUSTER STOPPED ===");
    }

    // -------------------------------------------------------
    // MAIN ROUNDTRIP TEST
    // -------------------------------------------------------
    @Test
    void put_get_roundTrip_realServers() throws Exception {

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
        assertTrue(putOk, "PUT should have successfully reached ACK quorum");

        log("[WORKER] GET starting...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "realA")) {

            byte[] got = in.readAllBytes();

            log("[WORKER] GET received " + got.length + " bytes");

            assertArrayEquals(data, got);
        }

        log("Round-trip test completed successfully.");
    }

    // -------------------------------------------------------
    // ERASURE TESTS (real servers)
    // -------------------------------------------------------

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