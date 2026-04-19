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
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;
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
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the multipart upload lifecycle running against a real
 * 9-node storage cluster.
 *
 * <p>Each test spins up the full stack: StorageNodeServerV2 instances with RocksDB,
 * in-memory PubSub/Metadata planes, a CommitCoordinator, a StorageWorker, and a
 * MultipartUploadManager backed by a MemoryCacheClient.
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
        for (int i = 0; i < 3; i++) parts.add(i);
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
        int numParts = 3;

        byte[][] partsForUpload1 = generateParts(numParts, partSize);
        byte[][] partsForUpload2 = generateParts(numParts, partSize);

        // Initiate two separate uploads before the race begins
        String uploadId1 = multipartManager.initiateMultipartUpload(BUCKET, key1);
        String uploadId2 = multipartManager.initiateMultipartUpload(BUCKET, key2);
        assertNotEquals(uploadId1, uploadId2, "Upload IDs should be unique");

        // Gate ensures both threads start their work at the same instant
        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<MultipartUploadResult> future1 = pool.submit(() -> {
            gate.await();
            for (int i = 0; i < numParts; i++) {
                int partNumber = i + 1;
                log("[THREAD-1] Uploading part " + partNumber);
                MultipartUploadResult r = multipartManager.uploadPart(
                        BUCKET, key1, uploadId1, partNumber,
                        new ByteArrayInputStream(partsForUpload1[i]),
                        partsForUpload1[i].length);
                assertTrue(r.isSuccess(),
                        "Upload1 part " + partNumber + " should succeed: " + r.message());
            }
            List<StorageService.CompletedPart> parts = new ArrayList<>();
            for (int i = 1; i <= numParts; i++) parts.add(new StorageService.CompletedPart(i));
            return multipartManager.completeMultipartUpload(BUCKET, key1, uploadId1, parts);
        });

        Future<MultipartUploadResult> future2 = pool.submit(() -> {
            gate.await();
            for (int i = 0; i < numParts; i++) {
                int partNumber = i + 1;
                log("[THREAD-2] Uploading part " + partNumber);
                MultipartUploadResult r = multipartManager.uploadPart(
                        BUCKET, key2, uploadId2, partNumber,
                        new ByteArrayInputStream(partsForUpload2[i]),
                        partsForUpload2[i].length);
                assertTrue(r.isSuccess(),
                        "Upload2 part " + partNumber + " should succeed: " + r.message());
            }
            List<StorageService.CompletedPart> parts = new ArrayList<>();
            for (int i = 1; i <= numParts; i++) parts.add(new StorageService.CompletedPart(i));
            return multipartManager.completeMultipartUpload(BUCKET, key2, uploadId2, parts);
        });

        // Release both threads simultaneously
        gate.countDown();

        // Propagate any assertion errors or exceptions from the worker threads
        MultipartUploadResult complete1 = future1.get(30, TimeUnit.SECONDS);
        MultipartUploadResult complete2 = future2.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(complete1.isSuccess(), "First upload complete should succeed");
        assertTrue(complete2.isSuccess(), "Second upload complete should succeed");

        // Verify each upload's data is independent
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey1 = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key1, uploadId1, partNumber);
            String partKey2 = com.github.koop.queryprocessor.processor.cache.MultipartUploadSession
                    .partStorageKey(BUCKET, key2, uploadId2, partNumber);

            try (InputStream in1 = worker.get(UUID.randomUUID(), BUCKET, partKey1)) {
                assertArrayEquals(partsForUpload1[i], in1.readAllBytes(),
                        "Upload 1 part " + partNumber + " data should be isolated");
            }
            try (InputStream in2 = worker.get(UUID.randomUUID(), BUCKET, partKey2)) {
                assertArrayEquals(partsForUpload2[i], in2.readAllBytes(),
                        "Upload 2 part " + partNumber + " data should be isolated");
            }
        }

        log("Concurrent uploads isolation test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: 10 concurrent multipart uploads to the same key — consistent state
    // -------------------------------------------------------
    @Test
    void multipartUpload_tenConcurrentUploadsToSameKey_consistentState() throws Exception {
        String key = "contested/same-key.bin";
        int partSize = 128 * 1024;
        int numUploads = 10;
        int numParts = 3;

        // Pre-generate data and initiate all uploads before the race
        byte[][][] allPartData = new byte[numUploads][][];
        String[] uploadIds = new String[numUploads];
        for (int u = 0; u < numUploads; u++) {
            allPartData[u] = generateParts(numParts, partSize);
            uploadIds[u] = multipartManager.initiateMultipartUpload(BUCKET, key);
        }

        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(numUploads);

        // Each thread: upload all parts → complete
        @SuppressWarnings("unchecked")
        Future<MultipartUploadResult>[] futures = new Future[numUploads];
        for (int u = 0; u < numUploads; u++) {
            final int idx = u;
            futures[u] = pool.submit(() -> {
                gate.await();
                for (int p = 0; p < numParts; p++) {
                    int partNumber = p + 1;
                    MultipartUploadResult r = multipartManager.uploadPart(
                            BUCKET, key, uploadIds[idx], partNumber,
                            new ByteArrayInputStream(allPartData[idx][p]),
                            allPartData[idx][p].length);
                    if (!r.isSuccess()) {
                        log("[UPLOAD-" + idx + "] Part " + partNumber + " failed: " + r.message());
                        return r;
                    }
                }
                List<StorageService.CompletedPart> parts = new ArrayList<>();
                for (int p = 1; p <= numParts; p++) parts.add(new StorageService.CompletedPart(p));
                return multipartManager.completeMultipartUpload(BUCKET, key, uploadIds[idx], parts);
            });
        }

        // Release all 10 threads simultaneously
        gate.countDown();

        // Collect results
        int successes = 0;
        List<Integer> succeededIdxs = new ArrayList<>();
        for (int u = 0; u < numUploads; u++) {
            MultipartUploadResult result = futures[u].get(60, TimeUnit.SECONDS);
            if (result.isSuccess()) {
                successes++;
                succeededIdxs.add(u);
            }
            log("[UPLOAD-" + u + "] Result: " + result.status() + " - " + result.message());
        }
        pool.shutdown();

        // At least one upload should have succeeded
        assertTrue(successes >= 1,
                "At least one concurrent upload should complete successfully, got " + successes);
        log(successes + " out of " + numUploads + " uploads completed successfully");

        // Every successful upload's parts should be readable and match its original data
        for (int idx : succeededIdxs) {
            for (int p = 0; p < numParts; p++) {
                int partNumber = p + 1;
                String partKey = MultipartUploadSession
                        .partStorageKey(BUCKET, key, uploadIds[idx], partNumber);
                try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                    byte[] got = in.readAllBytes();
                    assertArrayEquals(allPartData[idx][p], got,
                            "Upload " + idx + " part " + partNumber + " data should be intact");
                }
            }
        }

        log("10 concurrent uploads to same key — consistent state test completed.");
    }

    // -------------------------------------------------------
    // TEST: Abort races against active part uploads
    // -------------------------------------------------------
    @Test
    void multipartUpload_abortDuringActiveUploads_noCorruption() throws Exception {
        String key = "race/abort-vs-upload.bin";
        int partSize = 256 * 1024;
        int numParts = 6;

        byte[][] partData = generateParts(numParts, partSize);
        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        // Upload 2 parts sequentially so there's something to abort
        for (int i = 0; i < 2; i++) {
            MultipartUploadResult r = multipartManager.uploadPart(
                    BUCKET, key, uploadId, i + 1,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(r.isSuccess(), "Initial part " + (i + 1) + " should succeed");
        }

        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        // Thread 1: keep uploading remaining parts
        Future<List<MultipartUploadResult>> uploaderFuture = pool.submit(() -> {
            gate.await();
            List<MultipartUploadResult> results = new ArrayList<>();
            for (int i = 2; i < numParts; i++) {
                int partNumber = i + 1;
                log("[UPLOADER] Uploading part " + partNumber);
                MultipartUploadResult r = multipartManager.uploadPart(
                        BUCKET, key, uploadId, partNumber,
                        new ByteArrayInputStream(partData[i]), partData[i].length);
                results.add(r);
                log("[UPLOADER] Part " + partNumber + ": " + r.status());
            }
            return results;
        });

        // Thread 2: abort the upload
        Future<MultipartUploadResult> abortFuture = pool.submit(() -> {
            gate.await();
            log("[ABORTER] Issuing abort");
            return multipartManager.abortMultipartUpload(BUCKET, key, uploadId);
        });

        gate.countDown();

        MultipartUploadResult abortResult = abortFuture.get(30, TimeUnit.SECONDS);
        List<MultipartUploadResult> uploadResults = uploaderFuture.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(abortResult.isSuccess(), "Abort should succeed");

        // After abort, some uploads may have succeeded (raced before abort) and some
        // should have failed. Either way, completing the upload must fail.
        MultipartUploadResult completeAfterAbort = multipartManager.completeMultipartUpload(
                BUCKET, key, uploadId,
                List.of(new StorageService.CompletedPart(1)));
        assertFalse(completeAfterAbort.isSuccess(),
                "Complete after abort should fail");

        // No new parts should be uploadable
        MultipartUploadResult lateUpload = multipartManager.uploadPart(
                BUCKET, key, uploadId, numParts + 1,
                new ByteArrayInputStream(randomBytes(partSize)), partSize);
        assertFalse(lateUpload.isSuccess(),
                "Upload after abort should fail");

        log("Abort-vs-upload race test completed. Abort result: " + abortResult.status()
                + ", upload results: " + uploadResults.stream()
                .map(r -> r.status().name()).reduce((a, b) -> a + "," + b).orElse("none"));
    }

    // -------------------------------------------------------
    // TEST: Two threads race to complete the same upload — only one wins
    // -------------------------------------------------------
    @Test
    void multipartUpload_concurrentComplete_onlyOneSucceeds() throws Exception {
        String key = "race/double-complete.bin";
        int partSize = 128 * 1024;
        int numParts = 3;

        byte[][] partData = generateParts(numParts, partSize);
        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        // Upload all parts sequentially
        for (int i = 0; i < numParts; i++) {
            MultipartUploadResult r = multipartManager.uploadPart(
                    BUCKET, key, uploadId, i + 1,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(r.isSuccess(), "Part " + (i + 1) + " should upload successfully");
        }

        List<StorageService.CompletedPart> completedParts = new ArrayList<>();
        for (int i = 1; i <= numParts; i++) completedParts.add(new StorageService.CompletedPart(i));

        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<MultipartUploadResult> completer1 = pool.submit(() -> {
            gate.await();
            log("[COMPLETER-1] Completing upload");
            return multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        });

        Future<MultipartUploadResult> completer2 = pool.submit(() -> {
            gate.await();
            log("[COMPLETER-2] Completing upload");
            return multipartManager.completeMultipartUpload(BUCKET, key, uploadId, completedParts);
        });

        gate.countDown();

        MultipartUploadResult result1 = completer1.get(30, TimeUnit.SECONDS);
        MultipartUploadResult result2 = completer2.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        log("[COMPLETER-1] Result: " + result1.status() + " - " + result1.message());
        log("[COMPLETER-2] Result: " + result2.status() + " - " + result2.message());

        // Exactly one should succeed, the other should get CONFLICT or NOT_FOUND
        int completeSuccesses = 0;
        if (result1.isSuccess()) completeSuccesses++;
        if (result2.isSuccess()) completeSuccesses++;

        assertEquals(1, completeSuccesses,
                "Exactly one concurrent complete should succeed, but got " + completeSuccesses
                        + " (r1=" + result1.status() + ", r2=" + result2.status() + ")");

        // The winning complete's parts should be readable
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            String partKey = MultipartUploadSession
                    .partStorageKey(BUCKET, key, uploadId, partNumber);
            try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, partKey)) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(partData[i], got,
                        "Part " + partNumber + " data should survive the race");
            }
        }

        log("Concurrent complete race test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Node failure mid-upload — partial upload fails gracefully
    // -------------------------------------------------------
    @Test
    void multipartUpload_nodeFailureMidUpload_failsGracefully() throws Exception {
        String key = "failure/mid-upload.bin";
        int partSize = 256 * 1024;

        byte[][] partData = generateParts(4, partSize);
        String uploadId = multipartManager.initiateMultipartUpload(BUCKET, key);

        // Upload first 2 parts successfully while cluster is healthy
        for (int i = 0; i < 2; i++) {
            MultipartUploadResult r = multipartManager.uploadPart(
                    BUCKET, key, uploadId, i + 1,
                    new ByteArrayInputStream(partData[i]), partData[i].length);
            assertTrue(r.isSuccess(),
                    "Part " + (i + 1) + " should succeed with healthy cluster");
        }

        // Kill 4 nodes (more than erasure tolerance) — simulates "dies right here"
        log("Killing 4 nodes mid-upload...");
        stopNodes(0, 1, 2, 3);

        // Attempt to upload part 3 — should fail due to insufficient nodes
        MultipartUploadResult r3 = multipartManager.uploadPart(
                BUCKET, key, uploadId, 3,
                new ByteArrayInputStream(partData[2]), partData[2].length);
        log("Part 3 after node failure: " + r3.status() + " - " + r3.message());
        assertFalse(r3.isSuccess(),
                "Part upload should fail when nodes exceed erasure tolerance (4 of 9 down)");

        // Completing with only the pre-failure parts should also fail since
        // the cluster is degraded beyond tolerance
        List<StorageService.CompletedPart> parts = List.of(
                new StorageService.CompletedPart(1),
                new StorageService.CompletedPart(2));
        MultipartUploadResult complete = multipartManager.completeMultipartUpload(
                BUCKET, key, uploadId, parts);
        log("Complete with partial parts: " + complete.status() + " - " + complete.message());
        assertFalse(complete.isSuccess(),
                "Complete should fail when cluster is degraded beyond erasure tolerance");

        // The session should still be manageable — abort is best-effort cleanup
        MultipartUploadResult abort = multipartManager.abortMultipartUpload(BUCKET, key, uploadId);
        assertTrue(abort.isSuccess(),
                "Abort should succeed even when nodes are down (best-effort cleanup)");

        log("Node failure mid-upload test completed successfully.");
    }

    // -------------------------------------------------------
    // TEST: Abandoned upload doesn't block new uploads to same key
    // -------------------------------------------------------
    @Test
    void multipartUpload_abandonedUpload_doesNotBlockNewUpload() throws Exception {
        String key = "reuse/same-key.bin";
        int partSize = 128 * 1024;

        byte[] abandonedPart = randomBytes(partSize);
        byte[] newPart = randomBytes(partSize);

        // Start an upload, upload a part, then just... walk away
        String abandonedUploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        MultipartUploadResult r = multipartManager.uploadPart(
                BUCKET, key, abandonedUploadId, 1,
                new ByteArrayInputStream(abandonedPart), abandonedPart.length);
        assertTrue(r.isSuccess(), "Abandoned upload's part 1 should succeed");
        log("Abandoned upload " + abandonedUploadId + " with 1 part (never completed/aborted)");

        // Start a fresh upload to the same bucket+key
        String freshUploadId = multipartManager.initiateMultipartUpload(BUCKET, key);
        assertNotEquals(abandonedUploadId, freshUploadId,
                "Fresh upload should get a new upload ID");

        // Upload and complete the fresh upload
        MultipartUploadResult freshUpload = multipartManager.uploadPart(
                BUCKET, key, freshUploadId, 1,
                new ByteArrayInputStream(newPart), newPart.length);
        assertTrue(freshUpload.isSuccess(), "Fresh upload part should succeed");

        MultipartUploadResult freshComplete = multipartManager.completeMultipartUpload(
                BUCKET, key, freshUploadId,
                List.of(new StorageService.CompletedPart(1)));
        assertTrue(freshComplete.isSuccess(),
                "Fresh upload complete should succeed despite abandoned upload");

        // Verify the fresh upload's data
        String freshPartKey = MultipartUploadSession
                .partStorageKey(BUCKET, key, freshUploadId, 1);
        try (InputStream in = worker.get(UUID.randomUUID(), BUCKET, freshPartKey)) {
            assertArrayEquals(newPart, in.readAllBytes(),
                    "Fresh upload data should be correct");
        }

        // The abandoned upload should still be abortable (cleanup)
        MultipartUploadResult abortAbandoned = multipartManager.abortMultipartUpload(
                BUCKET, key, abandonedUploadId);
        assertTrue(abortAbandoned.isSuccess(),
                "Aborting the abandoned upload should succeed");

        // After abort, the abandoned upload's parts should no longer be completable
        MultipartUploadResult lateComplete = multipartManager.completeMultipartUpload(
                BUCKET, key, abandonedUploadId,
                List.of(new StorageService.CompletedPart(1)));
        assertFalse(lateComplete.isSuccess(),
                "Completing the abandoned (aborted) upload should fail");

        log("Abandoned upload doesn't block new upload test completed successfully.");
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
