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
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.CommitCoordinator;
import com.github.koop.queryprocessor.processor.MultipartUploadManager;
import com.github.koop.queryprocessor.processor.MultipartUploadResult;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;
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

/**
 * Integration tests for the multipart upload lifecycle running against a real
 * 9-node storage cluster.
 *
 * <p>Each test spins up the full stack: StorageNodeServerV2 instances with RocksDB,
 * in-memory PubSub/Metadata planes, a CommitCoordinator, a StorageWorker, and a
 * MultipartUploadManager backed by a MemoryCacheClient.
 *
 * <p>The test matrix covers:
 * <ul>
 *   <li>Happy-path: initiate → upload parts → complete → GET each part shard</li>
 *   <li>Multi-part ordering: parts uploaded out of order are assembled correctly</li>
 *   <li>Abort: uploaded parts are cleaned up after abort</li>
 *   <li>Large file: a multi-MB file split across many parts</li>
 *   <li>Erasure tolerance: GET still works with 3 nodes down after multipart commit</li>
 *   <li>Error paths: duplicate part uploads, completing unknown uploads, etc.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultipartUploadIT {

    private static final int TOTAL_NODES = 9;
    private static final String BUCKET = "test-bucket";

    private final List<StorageNodeServerV2> servers = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private final List<Database> databases = new ArrayList<>();
    private List<InetSocketAddress> addrs;

    // Shared infrastructure
    private MemoryFetcher sharedFetcher;
    private MemoryPubSub sharedPubSub;
    private MetadataClient sharedMetadataClient;
    private PubSubClient sharedPubSubClient;
    private CommitCoordinator commitCoordinator;
    private StorageWorker worker;
    private MultipartUploadManager multipartManager;
    private MemoryCacheClient cache;

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
    void startCluster() throws Exception {
        log("=== STARTING STORAGE CLUSTER (MultipartUploadIT) ===");

        addrs = new ArrayList<>();
        sharedFetcher = new MemoryFetcher();
        sharedPubSub = new MemoryPubSub();

        // 1. Shared control plane
        sharedPubSubClient = new PubSubClient(sharedPubSub);
        sharedPubSubClient.start();

        sharedMetadataClient = new MetadataClient(sharedFetcher);
        sharedMetadataClient.start();

        // 2. QP CommitCoordinator & StorageWorker
        commitCoordinator = new CommitCoordinator(sharedPubSubClient, 0, 10);
        worker = new StorageWorker(sharedMetadataClient, commitCoordinator);

        // 3. In-memory cache + MultipartUploadManager
        cache = new MemoryCacheClient();
        multipartManager = new MultipartUploadManager(worker, cache);

        // 4. Start Storage Node Servers (V2)
        for (int i = 0; i < TOTAL_NODES; i++) {
            int port = freePort();
            Path dir = Files.createTempDirectory("mpu-storagenode-" + i + "-");

            Database db = new Database(new RocksDbStorageStrategy(dir.resolve("db").toString()));
            StorageNodeServerV2 server =
                    new StorageNodeServerV2(port, "127.0.0.1", db, dir.resolve("data"),
                            sharedMetadataClient, sharedPubSubClient);

            servers.add(server);
            dataDirs.add(dir);
            databases.add(db);

            server.start();
            addrs.add(new InetSocketAddress("127.0.0.1", port));
            log("[NODE " + i + "] READY on port " + port);
        }

        // 5. Populate cluster configuration
        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        ErasureSet es = new ErasureSet();
        es.setNumber(1);
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
        for (int i = 0; i < 99; i++) parts.add(i);
        ps.setPartitions(parts);
        psConfig.setPartitionSpread(List.of(ps));

        sharedFetcher.update(esConfig);
        sharedFetcher.update(psConfig);

        Thread.sleep(1000);
        log("=== CLUSTER READY ===");
    }

    // -------------------------------------------------------
    // Shutdown cluster
    // -------------------------------------------------------
    @AfterEach
    void stopCluster() throws Exception {
        log("=== STOPPING CLUSTER ===");

        if (worker != null) worker.shutdown();
        if (sharedMetadataClient != null) sharedMetadataClient.close();
        if (sharedPubSubClient != null) sharedPubSubClient.close();

        for (int i = 0; i < servers.size(); i++) {
            log("Stopping node " + i);
            servers.get(i).stop();
        }

        for (Database db : databases) {
            try { db.close(); } catch (Exception ignored) {}
        }

        for (Path d : dataDirs) deleteRecursive(d);

        servers.clear();
        databases.clear();
        dataDirs.clear();

        log("=== CLUSTER STOPPED ===");
    }

    // -------------------------------------------------------
    // TEST: Full multipart lifecycle (initiate → upload → complete → GET parts)
    // -------------------------------------------------------
    @Test
    void multipartUpload_fullLifecycle_roundTrip() throws Exception {
        String key = "photos/vacation.jpg";
        int partSize = 1024 * 1024; // 1 MB per part
        int numParts = 3;

        log("Generating " + numParts + " parts of " + partSize + " bytes each...");
        byte[][] partData = generateParts(numParts, partSize);

        // 1. Initiate
        log("[MPU] Initiating multipart upload...");
        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        assertNotNull(uploadId, "Upload ID should not be null");
        log("[MPU] Upload ID = " + uploadId);

        // 2. Upload parts
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            log("[MPU] Uploading part " + partNumber + "...");
            MultipartUploadResult result = multipartManager.uploadPart(
                    BUCKET, key, uploadId, partNumber,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(result.isSuccess(),
                    "Part " + partNumber + " upload should succeed, got: " + result.message());
        }

        // 3. Complete
        log("[MPU] Completing multipart upload...");
        List<StorageService.CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            completedParts.add(new StorageService.CompletedPart(i + 1));
        }
        MultipartUploadResult completeResult =
                multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        assertTrue(completeResult.isSuccess(),
                "Complete should succeed, got: " + completeResult.message());

        // 4. Verify each part can be retrieved from storage
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key, uploadId, partNumber);
            log("[MPU] GET part " + partNumber + " (key=" + partKey + ")...");

            try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(partData[i], got,
                        "Part " + partNumber + " content should match original");
                log("[MPU] Part " + partNumber + " verified (" + got.length + " bytes)");
            }
        }

        log("Full multipart lifecycle test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Parts uploaded out of order still complete correctly
    // -------------------------------------------------------
    @Test
    void multipartUpload_outOfOrderParts_completesCorrectly() throws Exception {
        String key = "docs/report.pdf";
        int partSize = 512 * 1024; // 512 KB per part
        int numParts = 4;

        byte[][] partData = generateParts(numParts, partSize);

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        log("[MPU] Upload ID = " + uploadId);

        // Upload parts in reverse order: 4, 3, 2, 1
        int[] uploadOrder = {4, 3, 2, 1};
        for (int partNumber : uploadOrder) {
            log("[MPU] Uploading part " + partNumber + " (out of order)...");
            MultipartUploadResult result = multipartManager.uploadPart(
                    BUCKET, key, uploadId, partNumber,
                    new ByteArrayInputStream(partData[partNumber - 1]),
                    partData[partNumber - 1].length);
            assertTrue(result.isSuccess(),
                    "Part " + partNumber + " upload should succeed");
        }

        // Complete with parts listed in ascending order
        List<StorageService.CompletedPart> completedParts = new ArrayList<>();
        for (int i = 1; i <= numParts; i++) {
            completedParts.add(new StorageService.CompletedPart(i));
        }
        MultipartUploadResult completeResult =
                multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        assertTrue(completeResult.isSuccess(),
                "Complete should succeed for out-of-order uploads");

        // Verify each part
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key, uploadId, partNumber);
            try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(partData[i], got,
                        "Part " + partNumber + " content should match after out-of-order upload");
            }
        }

        log("Out-of-order parts test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Abort cleans up session state
    // -------------------------------------------------------
    @Test
    void multipartUpload_abort_cleansUpSessionState() throws Exception {
        String key = "videos/clip.mp4";
        int partSize = 256 * 1024;

        byte[] part1Data = randomBytes(partSize);
        byte[] part2Data = randomBytes(partSize);

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        log("[MPU] Upload ID = " + uploadId);

        // Upload 2 parts
        MultipartUploadResult r1 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 1,
                new ByteArrayInputStream(part1Data), part1Data.length);
        assertTrue(r1.isSuccess(), "Part 1 upload should succeed");

        MultipartUploadResult r2 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 2,
                new ByteArrayInputStream(part2Data), part2Data.length);
        assertTrue(r2.isSuccess(), "Part 2 upload should succeed");

        // Abort
        log("[MPU] Aborting multipart upload...");
        MultipartUploadResult abortResult = multipartManager.abortMultipartUpload(BUCKET, key, uploadId);
        assertTrue(abortResult.isSuccess(), "Abort should succeed");

        // Attempting to upload another part should fail (session no longer active)
        MultipartUploadResult r3 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 3,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertFalse(r3.isSuccess(),
                "Upload after abort should fail");
        log("[MPU] Post-abort upload correctly rejected: " + r3.message());

        // Completing the aborted upload should fail
        MultipartUploadResult completeResult = multipartManager.completeMultipartUpload(
                BUCKET, key, uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));
        assertFalse(completeResult.isSuccess(),
                "Complete after abort should fail");
        log("[MPU] Post-abort complete correctly rejected: " + completeResult.message());

        log("Abort test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Duplicate part number is rejected
    // -------------------------------------------------------
    @Test
    void multipartUpload_duplicatePartNumber_rejected() throws Exception {
        String key = "data/file.bin";
        int partSize = 128 * 1024;

        byte[] partData = randomBytes(partSize);

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        log("[MPU] Upload ID = " + uploadId);

        // Upload part 1
        MultipartUploadResult r1 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 1,
                new ByteArrayInputStream(partData), partData.length);
        assertTrue(r1.isSuccess(), "First upload of part 1 should succeed");

        // Try uploading part 1 again
        MultipartUploadResult r1Dup = multipartManager.uploadPart(
                BUCKET, key, uploadId, 1,
                new ByteArrayInputStream(partData), partData.length);
        assertFalse(r1Dup.isSuccess(),
                "Duplicate part 1 should be rejected");
        assertEquals(MultipartUploadResult.Status.CONFLICT, r1Dup.status(),
                "Duplicate part should return CONFLICT status");

        log("Duplicate part rejection test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Complete with missing part is rejected
    // -------------------------------------------------------
    @Test
    void multipartUpload_completeWithMissingPart_rejected() throws Exception {
        String key = "data/gaps.bin";
        int partSize = 128 * 1024;

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        // Upload only part 1
        MultipartUploadResult r1 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 1,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertTrue(r1.isSuccess());

        // Try to complete with parts 1 and 2, but 2 was never uploaded
        MultipartUploadResult completeResult = multipartManager.completeMultipartUpload(
                BUCKET, key, uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));
        assertFalse(completeResult.isSuccess(),
                "Complete with un-uploaded part should fail");
        assertEquals(MultipartUploadResult.Status.CONFLICT, completeResult.status());

        log("Complete-with-missing-part test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Complete/upload on unknown upload ID
    // -------------------------------------------------------
    @Test
    void multipartUpload_unknownUploadId_rejected() {
        String fakeUploadId = UUID.randomUUID().toString();
        int partSize = 128 * 1024;

        // Upload part with unknown upload ID
        MultipartUploadResult uploadResult = multipartManager.uploadPart(
                BUCKET, "key.txt", fakeUploadId, 1,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertFalse(uploadResult.isSuccess());
        assertEquals(MultipartUploadResult.Status.NOT_FOUND, uploadResult.status());

        // Complete with unknown upload ID
        MultipartUploadResult completeResult = multipartManager.completeMultipartUpload(
                BUCKET, "key.txt", fakeUploadId,
                List.of(new StorageService.CompletedPart(1)));
        assertFalse(completeResult.isSuccess());
        assertEquals(MultipartUploadResult.Status.NOT_FOUND, completeResult.status());

        log("Unknown upload ID rejection test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Abort of unknown upload ID is a graceful no-op
    // -------------------------------------------------------
    @Test
    void multipartUpload_abortUnknownUploadId_succeeds() {
        String fakeUploadId = UUID.randomUUID().toString();

        MultipartUploadResult result = multipartManager.abortMultipartUpload(
                BUCKET, "key.txt", fakeUploadId);
        assertTrue(result.isSuccess(),
                "Abort of unknown upload should succeed (graceful no-op)");

        log("Abort-unknown-upload-id no-op test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Large file with many small parts
    // -------------------------------------------------------
    @Test
    void multipartUpload_manyParts_roundTrip() throws Exception {
        String key = "archive/backup.tar.gz";
        int partSize = 256 * 1024; // 256 KB
        int numParts = 8;

        log("Testing multipart upload with " + numParts + " parts of " + partSize + " bytes...");
        byte[][] partData = generateParts(numParts, partSize);

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            MultipartUploadResult result = multipartManager.uploadPart(
                    BUCKET, key, uploadId, partNumber,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(result.isSuccess(),
                    "Part " + partNumber + " should upload successfully");
        }

        List<StorageService.CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            completedParts.add(new StorageService.CompletedPart(i + 1));
        }

        MultipartUploadResult completeResult =
                multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        assertTrue(completeResult.isSuccess(),
                "Complete should succeed for " + numParts + " parts");

        // Verify every part
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key, uploadId, partNumber);
            try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(partData[i], got,
                        "Part " + partNumber + " content should match");
            }
        }

        log("Many-parts roundtrip test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Multipart parts survive 3 node failures (erasure tolerance)
    // -------------------------------------------------------
    @Test
    void multipartUpload_toleratesThreeNodeFailures() throws Exception {
        String key = "resilient/data.bin";
        int partSize = 512 * 1024;
        int numParts = 2;

        byte[][] partData = generateParts(numParts, partSize);

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            MultipartUploadResult result = multipartManager.uploadPart(
                    BUCKET, key, uploadId, partNumber,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(result.isSuccess());
        }

        List<StorageService.CompletedPart> completedParts = List.of(
                new StorageService.CompletedPart(1),
                new StorageService.CompletedPart(2));
        MultipartUploadResult completeResult =
                multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        assertTrue(completeResult.isSuccess(), "Complete should succeed before node failures");

        // Kill 3 nodes
        log("Simulating 3 node failures...");
        stopNodes(0, 1, 2);

        // Verify parts can still be retrieved (erasure reconstruction)
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key, uploadId, partNumber);
            log("[MPU] GET part " + partNumber + " with 3 nodes down...");
            try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(partData[i], got,
                        "Part " + partNumber + " should reconstruct correctly with 3 nodes down");
                log("[MPU] Part " + partNumber + " reconstructed OK (" + got.length + " bytes)");
            }
        }

        log("Erasure tolerance test (3 failures) for multipart upload completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Bucket/key mismatch on part upload is rejected
    // -------------------------------------------------------
    @Test
    void multipartUpload_bucketKeyMismatch_rejected() throws Exception {
        String key = "correct/key.txt";
        int partSize = 128 * 1024;

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        // Attempt upload with wrong bucket
        MultipartUploadResult wrongBucket = multipartManager.uploadPart(
                "wrong-bucket", key, uploadId, 1,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertFalse(wrongBucket.isSuccess(),
                "Upload with wrong bucket should be rejected");
        assertEquals(MultipartUploadResult.Status.CONFLICT, wrongBucket.status());

        // Attempt upload with wrong key
        MultipartUploadResult wrongKey = multipartManager.uploadPart(
                BUCKET, "wrong/key.txt", uploadId, 1,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertFalse(wrongKey.isSuccess(),
                "Upload with wrong key should be rejected");
        assertEquals(MultipartUploadResult.Status.CONFLICT, wrongKey.status());

        log("Bucket/key mismatch rejection test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Multiple concurrent multipart uploads don't interfere
    // -------------------------------------------------------
    @Test
    void multipartUpload_multipleConcurrentUploads_isolated() throws Exception {
        String key1 = "upload1/file.bin";
        String key2 = "upload2/file.bin";
        int partSize = 256 * 1024;

        byte[] part1ForUpload1 = randomBytes(partSize);
        byte[] part1ForUpload2 = randomBytes(partSize);

        // Initiate two separate uploads
        String uploadId1 = multipartManager.initiateMultipartUpload(BUCKET, key1);
        String uploadId2 = multipartManager.initiateMultipartUpload(BUCKET, key2);

        assertNotEquals(uploadId1, uploadId2, "Upload IDs should be unique");

        // Upload parts to both
        assertTrue(multipartManager.uploadPart(
                BUCKET, key1, uploadId1, 1,
                new ByteArrayInputStream(part1ForUpload1), part1ForUpload1.length).isSuccess());
        assertTrue(multipartManager.uploadPart(
                BUCKET, key2, uploadId2, 1,
                new ByteArrayInputStream(part1ForUpload2), part1ForUpload2.length).isSuccess());

        // Complete both
        MultipartUploadResult complete1 = multipartManager.completeMultipartUpload(
                BUCKET, key1, uploadId1,
                List.of(new StorageService.CompletedPart(1)));
        assertTrue(complete1.isSuccess(), "First upload complete should succeed");

        MultipartUploadResult complete2 = multipartManager.completeMultipartUpload(
                BUCKET, key2, uploadId2,
                List.of(new StorageService.CompletedPart(1)));
        assertTrue(complete2.isSuccess(), "Second upload complete should succeed");

        // Verify each upload's data is independent
        String partKey1 = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                .partStorageKey(BUCKET, key1, uploadId1, 1);
        String partKey2 = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                .partStorageKey(BUCKET, key2, uploadId2, 1);

        try (InputStream in1 = worker.get(UUID.randomUUID(), BUCKET, partKey1)) {
            assertArrayEquals(part1ForUpload1, in1.readAllBytes(),
                    "Upload 1 part data should be isolated");
        }
        try (InputStream in2 = worker.get(UUID.randomUUID(), BUCKET, partKey2)) {
            assertArrayEquals(part1ForUpload2, in2.readAllBytes(),
                    "Upload 2 part data should be isolated");
        }

        log("Concurrent uploads isolation test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Single-part multipart upload (edge case)
    // -------------------------------------------------------
    @Test
    void multipartUpload_singlePart_roundTrip() throws Exception {
        String key = "tiny/single.bin";
        byte[] data = randomBytes(64 * 1024); // 64 KB

        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        MultipartUploadResult r = multipartManager.uploadPart(
                BUCKET, key, uploadId, 1,
                new ByteArrayInputStream(data), data.length);
        assertTrue(r.isSuccess());

        MultipartUploadResult complete = multipartManager.completeMultipartUpload(
                BUCKET, key, uploadId,
                List.of(new StorageService.CompletedPart(1)));
        assertTrue(complete.isSuccess());

        String partKey = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                .partStorageKey(BUCKET, key, uploadId, 1);
        try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
            assertArrayEquals(data, in.readAllBytes(),
                    "Single-part upload data should round-trip correctly");
        }

        log("Single-part multipart upload test completed successfully.");
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    private byte[][] generateParts(int numParts, int partSize) {
        byte[][] parts = new byte[numParts][];
        SecureRandom rng = new SecureRandom();
        for (int i = 0; i < numParts; i++) {
            parts[i] = new byte[partSize];
            rng.nextBytes(parts[i]);
        }
        return parts;
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new SecureRandom().nextBytes(data);
        return data;
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
