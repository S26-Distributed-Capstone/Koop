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
    private static final int DATA_SIZE = 2 * 1024 * 1024; // 2MB

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
    // GET: TOMBSTONE (DELETE) TEST
    // -------------------------------------------------------

    @Test
    void get_after_delete_returns_empty_realServers() throws Exception {

        log("Generating random test data for tombstone test...");
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        String key = "deleteMe";
        UUID putReq = UUID.randomUUID();

        log("[WORKER] PUT starting (deleteMe)...");
        boolean putOk = worker.put(putReq, "b", key, data.length, new ByteArrayInputStream(data));
        assertTrue(putOk, "PUT should succeed");

        // Verify GET works before delete
        log("[WORKER] GET starting (before delete)...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
            byte[] got = in.readAllBytes();
            assertArrayEquals(data, got, "GET should return correct data before delete");
        }

        // Read v1 sequence number and partition from DB
        var v1Version = databases.get(0).getLatestFileVersion("b/" + key);
        assertTrue(v1Version.isPresent(), "v1 version should exist in DB");
        long v1Seq = v1Version.get().sequenceNumber();

        var metadata = databases.get(0).getItem("b/" + key);
        assertTrue(metadata.isPresent(), "metadata should exist in DB");
        int partition = metadata.get().partition();

        // Write tombstone to ALL node DBs (simulating a delete commit)
        long tombstoneSeq = v1Seq + 1;
        log("Writing tombstone (seq=" + tombstoneSeq + ") to all " + databases.size() + " node DBs...");
        for (Database db : databases) {
            db.deleteItem("b/" + key, partition, tombstoneSeq);
        }

        // GET after delete should return empty (tombstone)
        log("[WORKER] GET starting (after delete, expect empty)...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
            byte[] got = in.readAllBytes();
            assertEquals(0, got.length, "GET after delete should return empty data (tombstone)");
        }

        log("Tombstone/delete test completed successfully.");
    }

    // -------------------------------------------------------
    // GET: MULTIPART ROUNDTRIP TEST
    // -------------------------------------------------------

    @Test
    void get_multipart_roundTrip_realServers() throws Exception {

        log("Generating chunk data for multipart test...");
        int chunkSize = DATA_SIZE / 2;
        byte[] chunk1Data = new byte[chunkSize];
        byte[] chunk2Data = new byte[chunkSize];
        new SecureRandom().nextBytes(chunk1Data);
        new SecureRandom().nextBytes(chunk2Data);

        // PUT chunk 1
        UUID chunk1Req = UUID.randomUUID();
        log("[WORKER] PUT chunk1 starting...");
        boolean put1Ok = worker.put(chunk1Req, "b", "part1", chunk1Data.length, new ByteArrayInputStream(chunk1Data));
        assertTrue(put1Ok, "PUT chunk1 should succeed");

        // PUT chunk 2
        UUID chunk2Req = UUID.randomUUID();
        log("[WORKER] PUT chunk2 starting...");
        boolean put2Ok = worker.put(chunk2Req, "b", "part2", chunk2Data.length, new ByteArrayInputStream(chunk2Data));
        assertTrue(put2Ok, "PUT chunk2 should succeed");

        // Commit multipart object referencing both chunks
        String uploadId = UUID.randomUUID().toString();
        log("[WORKER] beginMultipartCommit starting...");
        boolean commitOk = worker.beginMultipartCommit("b", "multipartFile", uploadId, List.of("b/part1", "b/part2"));
        assertTrue(commitOk, "Multipart commit should succeed");

        // GET the multipart object — should stream chunk1 then chunk2
        log("[WORKER] GET multipart starting...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "multipartFile")) {
            byte[] got = in.readAllBytes();

            byte[] expected = new byte[chunk1Data.length + chunk2Data.length];
            System.arraycopy(chunk1Data, 0, expected, 0, chunk1Data.length);
            System.arraycopy(chunk2Data, 0, expected, chunk1Data.length, chunk2Data.length);

            log("[WORKER] GET multipart received " + got.length + " bytes (expected " + expected.length + ")");
            assertArrayEquals(expected, got, "Multipart GET should return concatenated chunk data");
        }

        log("Multipart roundtrip test completed successfully.");
    }

    // -------------------------------------------------------
    // GET: VERSION FALLBACK TEST
    // -------------------------------------------------------

    @Test
    void get_blob_version_fallback_realServers() throws Exception {

        log("Generating random test data for version fallback test...");
        byte[] v1Data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(v1Data);

        String key = "fallbackKey";
        UUID v1Req = UUID.randomUUID();

        // PUT v1 normally (all 9 nodes receive shards)
        log("[WORKER] PUT v1 starting...");
        boolean putOk = worker.put(v1Req, "b", key, v1Data.length, new ByteArrayInputStream(v1Data));
        assertTrue(putOk, "PUT v1 should succeed");

        // Verify v1 is readable
        log("[WORKER] GET v1 starting (sanity check)...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
            assertArrayEquals(v1Data, in.readAllBytes(), "v1 should be readable");
        }

        // Read v1 sequence number and partition from the first node's DB
        var v1Version = databases.get(0).getLatestFileVersion("b/" + key);
        assertTrue(v1Version.isPresent(), "v1 version should exist in DB");
        long v1Seq = v1Version.get().sequenceNumber();

        var metadata = databases.get(0).getItem("b/" + key);
        assertTrue(metadata.isPresent(), "metadata should exist in DB");
        int partition = metadata.get().partition();

        // Simulate a partial v2 on only 3 nodes (< k=6)
        // Write fake blob files and v2 metadata entries to nodes 0, 1, 2 only
        String fakeV2RequestId = UUID.randomUUID().toString();
        long v2Seq = v1Seq + 1;
        log("Writing partial v2 (seq=" + v2Seq + ") to 3 nodes...");

        for (int i = 0; i < 3; i++) {
            // Write a fake blob file on disk so retrieve() finds it
            Path blobDir = dataDirs.get(i).resolve("data").resolve("blobs");
            String prefix = fakeV2RequestId.length() >= 3 ? fakeV2RequestId.substring(0, 3) : "000";
            Path blobPath = blobDir.resolve(prefix).resolve(fakeV2RequestId);
            Files.createDirectories(blobPath.getParent());
            Files.write(blobPath, new byte[]{0, 1, 2, 3}); // dummy shard data

            // Write v2 metadata to this node's DB
            databases.get(i).putItem("b/" + key, partition, v2Seq, fakeV2RequestId);
        }

        // GET should: find v2 has only 3 shards (< k=6), fall back to v1 (9 shards), reconstruct v1
        log("[WORKER] GET starting (expect v2 fallback to v1)...");
        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
            byte[] got = in.readAllBytes();
            log("[WORKER] GET received " + got.length + " bytes after version fallback");
            assertArrayEquals(v1Data, got, "Should reconstruct v1 data after v2 has insufficient shards");
        }

        log("Version fallback test completed successfully.");
    }

    // -------------------------------------------------------
    // BUCKET AND DELETE TESTS (real servers)
    // -------------------------------------------------------

    /**
     * Creates a bucket via pub/sub and verifies that the SN writes the bucket
     * record to RocksDB by checking that a subsequent put into that bucket succeeds.
     * This exercises the full CreateBucketMessage → SN commit → ACK path.
     */
    @Test
    void createBucket_realServers() throws Exception {
        log("Testing createBucket with real storage nodes...");

        UUID req = UUID.randomUUID();
        boolean ok = worker.createBucket(req, "integration-bucket");
        log("[WORKER] createBucket result = " + ok);
        assertTrue(ok, "createBucket should succeed with real SNs");

        // Confirm the bucket is usable by putting an object into it.
        byte[] data = new byte[1024];
        new SecureRandom().nextBytes(data);
        boolean putOk = worker.put(UUID.randomUUID(), "integration-bucket", "probe",
                data.length, new ByteArrayInputStream(data));
        assertTrue(putOk, "PUT into newly created bucket should succeed");

        log("createBucket test completed successfully.");
    }

    /**
     * Creates a bucket, puts an object, then deletes the bucket via pub/sub.
     * Verifies that the SN tombstones the bucket record in RocksDB.
     */
    @Test
    void deleteBucket_realServers() throws Exception {
        log("Testing deleteBucket with real storage nodes...");

        // Create bucket first.
        assertTrue(worker.createBucket(UUID.randomUUID(), "doomed-bucket"),
                "createBucket should succeed");

        // Put an object so there is real data behind the bucket.
        byte[] data = new byte[1024];
        new SecureRandom().nextBytes(data);
        assertTrue(worker.put(UUID.randomUUID(), "doomed-bucket", "obj",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed before bucket deletion");

        // Delete the bucket.
        boolean deleteOk = worker.deleteBucket(UUID.randomUUID(), "doomed-bucket");
        log("[WORKER] deleteBucket result = " + deleteOk);
        assertTrue(deleteOk, "deleteBucket should succeed with real SNs");

        log("deleteBucket test completed successfully.");
    }

    /**
     * Verifies the full bucket lifecycle with real SNs:
     * create bucket → put object → get object → delete object → delete bucket.
     *
     * After delete the object should be tombstoned in RocksDB on each SN,
     * so a subsequent GET should return a tombstone (404 from the SN),
     * causing reconstruction to fail or return no data.
     */
    @Test
    void bucketLifecycle_realServers() throws Exception {
        log("Testing full bucket lifecycle with real storage nodes...");

        String bucket = "lifecycle-bucket";

        // 1. Create bucket.
        assertTrue(worker.createBucket(UUID.randomUUID(), bucket),
                "createBucket should succeed");
        log("Bucket created.");

        // 2. PUT object.
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);
        assertTrue(worker.put(UUID.randomUUID(), bucket, "life-obj",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed");
        log("Object PUT succeeded.");

        // 3. GET object — must round-trip correctly.
        try (InputStream in = worker.get(UUID.randomUUID(), bucket, "life-obj")) {
            assertArrayEquals(data, in.readAllBytes(), "GET should return original data");
        }
        log("GET round-trip verified.");

        // 4. DELETE object via pub/sub — SNs tombstone metadata + op-log.
        assertTrue(worker.delete(UUID.randomUUID(), bucket, "life-obj"),
                "delete should succeed");
        log("Object deleted.");

        // 5. GET after delete — SNs return tombstone (404), so reconstruction fails.
        // We accept either an exception or an empty/short stream.
        try (InputStream in = worker.get(UUID.randomUUID(), bucket, "life-obj")) {
            byte[] got = in.readAllBytes();
            assertNotEquals(data.length, got.length,
                    "Data should not be fully retrievable after delete");
        } catch (Exception e) {
            log("[WORKER] GET after delete threw as expected: " + e.getMessage());
        }
        log("Post-delete GET correctly failed.");

        // 6. DELETE bucket.
        assertTrue(worker.deleteBucket(UUID.randomUUID(), bucket),
                "deleteBucket should succeed");
        log("Bucket deleted.");

        log("Full bucket lifecycle test completed successfully.");
    }

    /**
     * Verifies that delete() correctly tombstones an object across the real SN
     * cluster and that a subsequent GET cannot reconstruct it.
     *
     * Specifically tests the pub/sub delete path: QP publishes DeleteMessage →
     * each SN writes a tombstone to RocksDB → SN ACKs → QP returns true.
     */
    @Test
    void delete_tombstones_object_realServers() throws Exception {
        log("Testing pub/sub delete with real storage nodes...");

        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        // PUT first.
        assertTrue(worker.put(UUID.randomUUID(), "b", "to-delete",
                        data.length, new ByteArrayInputStream(data)),
                "PUT should succeed");
        log("Object PUT succeeded.");

        // Verify it's readable.
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "to-delete")) {
            assertArrayEquals(data, in.readAllBytes(), "Object should be readable before delete");
        }

        // DELETE via pub/sub.
        boolean deleteOk = worker.delete(UUID.randomUUID(), "b", "to-delete");
        log("[WORKER] delete result = " + deleteOk);
        assertTrue(deleteOk, "delete should succeed with real SNs");

        // GET after delete — tombstone on SNs means reconstruction fails.
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "to-delete")) {
            byte[] got = in.readAllBytes();
            assertNotEquals(data.length, got.length,
                    "Object should not be fully retrievable after tombstone");
        } catch (Exception e) {
            log("[WORKER] GET after delete threw as expected: " + e.getMessage());
        }

        log("Delete tombstone test completed successfully.");
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