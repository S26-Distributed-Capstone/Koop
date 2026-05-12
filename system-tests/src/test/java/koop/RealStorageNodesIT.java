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
import com.github.koop.storagenode.RocksDbRepairQueue;
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
    private static final int DATA_SIZE = 2 * 1024 * 1024; // 2MB

    private final List<StorageNodeServerV2> servers = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private final List<Database> databases = new ArrayList<>();
    private List<InetSocketAddress> addrs;

    private MemoryFetcher sharedFetcher;
    private MemoryPubSub sharedPubSub;
    private MetadataClient sharedMetadataClient;
    private PubSubClient sharedPubSubClient;
    private CommitCoordinator commitCoordinator;
    private StorageWorker worker;

    private static void log(String msg) {
        System.out.printf("[%s] %s%n",
                LocalTime.now().withNano(0),
                msg);
    }

    @BeforeEach
    void startRealNodes() throws Exception {

        log("=== STARTING STORAGE CLUSTER ===");

        addrs = new ArrayList<>();
        sharedFetcher = new MemoryFetcher();
        sharedPubSub = new MemoryPubSub();

        sharedPubSubClient = new PubSubClient(sharedPubSub);
        sharedPubSubClient.start();

        sharedMetadataClient = new MetadataClient(sharedFetcher);
        sharedMetadataClient.start();

        commitCoordinator = new CommitCoordinator(sharedPubSubClient, 0, 10);
        worker = new StorageWorker(sharedMetadataClient, commitCoordinator);

        for (int i = 0; i < TOTAL_NODES; i++) {

            int port = freePort();
            Path dir = Files.createTempDirectory("storagenode-" + i + "-");

            log("Starting node " + i + " on port " + port);

            RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(dir.resolve("db").toString());
            RocksDbRepairQueue repairQueue = new RocksDbRepairQueue(strategy);
            Database db = new Database(strategy);
            StorageNodeServerV2 server =
                    new StorageNodeServerV2(port, "127.0.0.1", db, dir.resolve("data"), sharedMetadataClient, sharedPubSubClient, repairQueue);

            servers.add(server);
            dataDirs.add(dir);
            databases.add(db);

            log("[NODE " + i + "] server starting");

            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);

            log("[NODE " + i + "] READY");

            addrs.add(addr);
        }

        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        ErasureSet es = new ErasureSet();
        es.setNumber(1);
        es.setN(9);
        es.setM(6);
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
        for(int i = 0; i < 3; i++) parts.add(i);
        ps.setPartitions(parts);
        psConfig.setPartitionSpread(List.of(ps));

        sharedFetcher.update(esConfig);
        sharedFetcher.update(psConfig);

        servers.forEach(StorageNodeServerV2::start);

        log("=== CLUSTER READY ===");
    }

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
        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", "realA");
        assertNotNull(obj);
        try (InputStream in = obj.stream()) {

            byte[] got = in.readAllBytes();

            log("[WORKER] GET received " + got.length + " bytes");

            assertArrayEquals(data, got);
        }

        log("Round-trip test completed successfully.");
    }

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

        log("Simulating 3 node failures (should still reconstruct)...");
        stopNodes(0, 1, 2);

        log("[WORKER] GET starting with 3 nodes down...");
        byte[] got;
        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", key);
        assertNotNull(obj);
        try (InputStream in = obj.stream()) {
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

        log("Simulating 4 node failures (should NOT reconstruct)...");
        stopNodes(0, 1, 2, 3);

        log("[WORKER] GET starting with 4 nodes down (expect failure)...");
        try {
            StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", key);
            if (obj != null) {
                try (InputStream in = obj.stream()) {
                    byte[] got = in.readAllBytes();
                    log("[WORKER] GET returned " + got.length + " bytes with 4 nodes down");

                    if (Arrays.equals(data, got)) {
                        fail("Expected reconstruction to fail with 4 nodes down, but got full correct data");
                    }
                    assertNotEquals(data.length, got.length,
                            "Expected missing/corrupt data length with 4 nodes down");
                }
            } else {
                log("[WORKER] GET returned null with 4 nodes down (as expected)");
            }
        } catch (Exception e) {
            log("[WORKER] GET failed with exception as expected: " + e);
        }

        log("Erasure failure test (4 failures) completed.");
    }

    @Test
    void get_after_delete_returnsNull_realServers() throws Exception {

        log("Generating random test data for tombstone test...");
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        String key = "deleteMe";
        UUID putReq = UUID.randomUUID();

        log("[WORKER] PUT starting (deleteMe)...");
        boolean putOk = worker.put(putReq, "b", key, data.length, new ByteArrayInputStream(data));
        assertTrue(putOk, "PUT should succeed");

        log("[WORKER] GET starting (before delete)...");
        StorageWorker.RetrievedObject beforeDelete = worker.get(UUID.randomUUID(), "b", key);
        assertNotNull(beforeDelete);
        try (InputStream in = beforeDelete.stream()) {
            byte[] got = in.readAllBytes();
            assertArrayEquals(data, got, "GET should return correct data before delete");
        }

        var v1Version = databases.get(0).getLatestFileVersion("b/" + key);
        assertTrue(v1Version.isPresent(), "v1 version should exist in DB");
        long v1Seq = v1Version.get().sequenceNumber();

        var metadata = databases.get(0).getItem("b/" + key);
        assertTrue(metadata.isPresent(), "metadata should exist in DB");
        int partition = metadata.get().partition();

        long tombstoneSeq = v1Seq + 1;
        log("Writing tombstone (seq=" + tombstoneSeq + ") to all " + databases.size() + " node DBs...");
        for (Database db : databases) {
            db.deleteItem("b/" + key, partition, tombstoneSeq);
        }

        log("[WORKER] GET starting (after delete, expect null)...");
        StorageWorker.RetrievedObject afterDelete = worker.get(UUID.randomUUID(), "b", key);
        assertNull(afterDelete, "GET after delete should return null");

        log("Tombstone/delete test completed successfully.");
    }

    @Test
    void get_multipart_roundTrip_realServers() throws Exception {

        log("Generating chunk data for multipart test...");
        int chunkSize = DATA_SIZE / 2;
        byte[] chunk1Data = new byte[chunkSize];
        byte[] chunk2Data = new byte[chunkSize];
        new SecureRandom().nextBytes(chunk1Data);
        new SecureRandom().nextBytes(chunk2Data);

        UUID chunk1Req = UUID.randomUUID();
        log("[WORKER] PUT chunk1 starting...");
        boolean put1Ok = worker.put(chunk1Req, "b", "part1", chunk1Data.length, new ByteArrayInputStream(chunk1Data));
        assertTrue(put1Ok, "PUT chunk1 should succeed");

        UUID chunk2Req = UUID.randomUUID();
        log("[WORKER] PUT chunk2 starting...");
        boolean put2Ok = worker.put(chunk2Req, "b", "part2", chunk2Data.length, new ByteArrayInputStream(chunk2Data));
        assertTrue(put2Ok, "PUT chunk2 should succeed");

        String uploadId = UUID.randomUUID().toString();
        log("[WORKER] beginMultipartCommit starting...");
        long totalSize = chunk1Data.length + chunk2Data.length;
        boolean commitOk = worker.beginMultipartCommit("b", "multipartFile", uploadId, List.of("b/part1", "b/part2"), totalSize);
        assertTrue(commitOk, "Multipart commit should succeed");

        log("[WORKER] GET multipart starting...");
        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", "multipartFile");
        assertNotNull(obj);
        try (InputStream in = obj.stream()) {
            byte[] got = in.readAllBytes();

            byte[] expected = new byte[chunk1Data.length + chunk2Data.length];
            System.arraycopy(chunk1Data, 0, expected, 0, chunk1Data.length);
            System.arraycopy(chunk2Data, 0, expected, chunk1Data.length, chunk2Data.length);

            log("[WORKER] GET multipart received " + got.length + " bytes (expected " + expected.length + ")");
            assertArrayEquals(expected, got, "Multipart GET should return concatenated chunk data");
        }

        log("Multipart roundtrip test completed successfully.");
    }

    @Test
    void get_blob_version_fallback_realServers() throws Exception {

        log("Generating random test data for version fallback test...");
        byte[] v1Data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(v1Data);

        String key = "fallbackKey";
        UUID v1Req = UUID.randomUUID();

        log("[WORKER] PUT v1 starting...");
        boolean putOk = worker.put(v1Req, "b", key, v1Data.length, new ByteArrayInputStream(v1Data));
        assertTrue(putOk, "PUT v1 should succeed");

        log("[WORKER] GET v1 starting (sanity check)...");
        StorageWorker.RetrievedObject obj1 = worker.get(UUID.randomUUID(), "b", key);
        assertNotNull(obj1);
        try (InputStream in = obj1.stream()) {
            assertArrayEquals(v1Data, in.readAllBytes(), "v1 should be readable");
        }

        var v1Version = databases.get(0).getLatestFileVersion("b/" + key);
        assertTrue(v1Version.isPresent(), "v1 version should exist in DB");
        long v1Seq = v1Version.get().sequenceNumber();

        var metadata = databases.get(0).getItem("b/" + key);
        assertTrue(metadata.isPresent(), "metadata should exist in DB");
        int partition = metadata.get().partition();

        String fakeV2RequestId = UUID.randomUUID().toString();
        long v2Seq = v1Seq + 1;
        log("Writing partial v2 (seq=" + v2Seq + ") to 3 nodes...");

        for (int i = 0; i < 3; i++) {
            Path blobDir = dataDirs.get(i).resolve("data").resolve("blobs");
            String prefix = fakeV2RequestId.length() >= 3 ? fakeV2RequestId.substring(0, 3) : "000";
            Path blobPath = blobDir.resolve(prefix).resolve(fakeV2RequestId);
            Files.createDirectories(blobPath.getParent());
            Files.write(blobPath, new byte[]{0, 1, 2, 3});

            databases.get(i).putItem("b/" + key, partition, v2Seq, fakeV2RequestId, v1Data.length);
        }

        log("[WORKER] GET starting (expect v2 fallback to v1)...");
        StorageWorker.RetrievedObject obj2 = worker.get(UUID.randomUUID(), "b", key);
        assertNotNull(obj2);
        try (InputStream in = obj2.stream()) {
            byte[] got = in.readAllBytes();
            log("[WORKER] GET received " + got.length + " bytes after version fallback");
            assertArrayEquals(v1Data, got, "Should reconstruct v1 data after v2 has insufficient shards");
        }

        log("Version fallback test completed successfully.");
    }

    @Test
    void createBucket_realServers() throws Exception {
        log("Testing createBucket with real storage nodes...");

        UUID req = UUID.randomUUID();
        boolean ok = worker.createBucket(req, "integration-bucket");
        log("[WORKER] createBucket result = " + ok);
        assertTrue(ok, "createBucket should succeed with real SNs");

        byte[] data = new byte[1024];
        new SecureRandom().nextBytes(data);
        boolean putOk = worker.put(UUID.randomUUID(), "integration-bucket", "probe",
                data.length, new ByteArrayInputStream(data));
        assertTrue(putOk, "PUT into newly created bucket should succeed");

        log("createBucket test completed successfully.");
    }

    @Test
    void deleteBucket_realServers() throws Exception {
        log("Testing deleteBucket with real storage nodes...");

        assertTrue(worker.createBucket(UUID.randomUUID(), "doomed-bucket"),
                "createBucket should succeed");

        byte[] data = new byte[1024];
        new SecureRandom().nextBytes(data);
        assertTrue(worker.put(UUID.randomUUID(), "doomed-bucket", "obj",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed before bucket deletion");

        boolean deleteOk = worker.deleteBucket(UUID.randomUUID(), "doomed-bucket");
        log("[WORKER] deleteBucket result = " + deleteOk);
        assertTrue(deleteOk, "deleteBucket should succeed with real SNs");

        log("deleteBucket test completed successfully.");
    }

    @Test
    void bucketLifecycle_realServers() throws Exception {
        log("Testing full bucket lifecycle with real storage nodes...");

        String bucket = "lifecycle-bucket";

        assertTrue(worker.createBucket(UUID.randomUUID(), bucket),
                "createBucket should succeed");
        log("Bucket created.");

        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);
        assertTrue(worker.put(UUID.randomUUID(), bucket, "life-obj",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed");
        log("Object PUT succeeded.");

        StorageWorker.RetrievedObject obj1 = worker.get(UUID.randomUUID(), bucket, "life-obj");
        assertNotNull(obj1);
        try (InputStream in = obj1.stream()) {
            assertArrayEquals(data, in.readAllBytes(), "GET should return original data");
        }
        log("GET round-trip verified.");

        assertTrue(worker.delete(UUID.randomUUID(), bucket, "life-obj"),
                "delete should succeed");
        log("Object deleted.");

        try {
            StorageWorker.RetrievedObject obj2 = worker.get(UUID.randomUUID(), bucket, "life-obj");
            if (obj2 != null) {
                try (InputStream in = obj2.stream()) {
                    byte[] got = in.readAllBytes();
                    assertNotEquals(data.length, got.length,
                            "Data should not be fully retrievable after delete");
                }
            } else {
                log("GET returned null as expected after delete.");
            }
        } catch (Exception e) {
            log("[WORKER] GET after delete threw as expected: " + e.getMessage());
        }
        log("Post-delete GET correctly failed.");

        assertTrue(worker.deleteBucket(UUID.randomUUID(), bucket),
                "deleteBucket should succeed");
        log("Bucket deleted.");

        log("Full bucket lifecycle test completed successfully.");
    }

    @Test
    void delete_tombstones_object_realServers() throws Exception {
        log("Testing pub/sub delete with real storage nodes...");

        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        assertTrue(worker.put(UUID.randomUUID(), "b", "to-delete",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed");
        log("Object PUT succeeded.");

        StorageWorker.RetrievedObject obj1 = worker.get(UUID.randomUUID(), "b", "to-delete");
        assertNotNull(obj1);
        try (InputStream in = obj1.stream()) {
            assertArrayEquals(data, in.readAllBytes(), "Object should be readable before delete");
        }

        boolean deleteOk = worker.delete(UUID.randomUUID(), "b", "to-delete");
        log("[WORKER] delete result = " + deleteOk);
        assertTrue(deleteOk, "delete should succeed with real SNs");

        try {
            StorageWorker.RetrievedObject obj2 = worker.get(UUID.randomUUID(), "b", "to-delete");
            if (obj2 != null) {
                try (InputStream in = obj2.stream()) {
                    byte[] got = in.readAllBytes();
                    assertNotEquals(data.length, got.length,
                            "Object should not be fully retrievable after tombstone");
                }
            }
        } catch (Exception e) {
            log("[WORKER] GET after delete threw as expected: " + e.getMessage());
        }

        log("Delete tombstone test completed successfully.");
    }

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