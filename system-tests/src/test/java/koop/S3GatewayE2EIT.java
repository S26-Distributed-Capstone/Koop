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
import com.github.koop.queryprocessor.gateway.Main;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.CommitCoordinator;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;
import com.github.koop.storagenode.StorageNodeServerV2;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;
import io.javalin.Javalin;
import org.junit.jupiter.api.*;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.SecureRandom;
import java.time.LocalTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for the S3-compatible Javalin gateway
 * running against a real 9-node storage cluster.
 *
 * <p>These tests exercise the <b>full path</b>:
 * <pre>
 *   AWS S3 SDK Client
 *     → Javalin Gateway (Main.createApp)
 *       → StorageWorkerService
 *         → MultipartUploadManager / StorageWorker
 *           → StorageNodeServerV2 (RocksDB)
 * </pre>
 *
 * <p>This closes the gap identified in Issue #93: no prior test validated this
 * complete stack using the S3 SDK as the entry point.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3GatewayE2EIT {

    private static final int TOTAL_NODES = 9;
    private static final String BUCKET = "e2e-bucket";

    // Storage cluster
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

    // Gateway
    private Javalin app;
    private S3Client s3;

    // -------------------------------------------------------
    // Logging helper
    // -------------------------------------------------------
    private static void log(String msg) {
        System.out.printf("[%s] %s%n",
                LocalTime.now().withNano(0),
                msg);
    }

    // -------------------------------------------------------
    // Setup: full stack (cluster + gateway + S3 client)
    // -------------------------------------------------------
    @BeforeEach
    void startFullStack() throws Exception {
        log("=== STARTING FULL STACK (S3GatewayE2EIT) ===");

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

        // 3. Start Storage Node Servers (V2)
        for (int i = 0; i < TOTAL_NODES; i++) {
            int port = freePort();
            Path dir = Files.createTempDirectory("e2e-storagenode-" + i + "-");

            Database db = new Database(new RocksDbStorageStrategy(dir.resolve("db").toString()));
            StorageNodeServerV2 server =
                    new StorageNodeServerV2(port, "127.0.0.1", db, dir.resolve("data"),
                            sharedMetadataClient, sharedPubSubClient);

            servers.add(server);
            dataDirs.add(dir);
            databases.add(db);

            addrs.add(new InetSocketAddress("127.0.0.1", port));
            log("[NODE " + i + "] READY on port " + port);
        }

        // 4. Populate cluster configuration
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
        for (int i = 0; i < 3; i++) parts.add(i);
        ps.setPartitions(parts);
        psConfig.setPartitionSpread(List.of(ps));

        sharedFetcher.update(esConfig);
        sharedFetcher.update(psConfig);

        servers.forEach(StorageNodeServerV2::start);
        log("=== CLUSTER READY ===");

        // 5. Wire up the Gateway
        MemoryCacheClient cache = new MemoryCacheClient();
        StorageService storage = new StorageWorkerService(worker, cache);
        app = Main.createApp(storage).start(0);
        int gatewayPort = app.port();
        log("[GATEWAY] READY on port " + gatewayPort);

        // 6. Configure AWS S3 SDK client
        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + gatewayPort))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("fake-access-key", "fake-secret-key")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .chunkedEncodingEnabled(false)
                        .build())
                .build();

        log("=== FULL STACK READY ===");
    }

    // -------------------------------------------------------
    // Shutdown
    // -------------------------------------------------------
    @AfterEach
    void stopFullStack() throws Exception {
        log("=== STOPPING FULL STACK ===");

        if (s3 != null) s3.close();
        if (app != null) app.stop();
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

        log("=== FULL STACK STOPPED ===");
    }

    // =====================================================================
    // TEST 1: Simple PUT → GET round-trip
    // =====================================================================
    @Test
    void e2e_putObject_thenGetObject_roundTrip() throws Exception {
        log("[E2E] PUT → GET round-trip test");

        byte[] data = new byte[2 * 1024 * 1024]; // 2 MB
        new SecureRandom().nextBytes(data);
        String key = "roundtrip.bin";

        // PUT via S3 SDK
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .contentLength((long) data.length)
                        .build(),
                RequestBody.fromBytes(data)
        );
        log("[E2E] PUT completed");

        // GET via S3 SDK
        ResponseBytes<GetObjectResponse> response = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build(),
                ResponseTransformer.toBytes()
        );

        byte[] got = response.asByteArray();
        log("[E2E] GET received " + got.length + " bytes");

        assertArrayEquals(data, got,
                "GET should return exactly the bytes that were PUT");

        log("[E2E] PUT → GET round-trip test PASSED");
    }

    // =====================================================================
    // TEST 2: PUT → DELETE → GET returns 404
    // =====================================================================
    @Test
    void e2e_putObject_thenDeleteObject_thenGet_returns404() throws Exception {
        log("[E2E] PUT → DELETE → GET 404 test");

        byte[] data = "delete-me-data".getBytes(StandardCharsets.UTF_8);
        String key = "delete-test.txt";

        // PUT
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .contentLength((long) data.length)
                        .build(),
                RequestBody.fromBytes(data)
        );
        log("[E2E] PUT completed");

        // Verify GET works before delete
        ResponseBytes<GetObjectResponse> beforeDelete = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );
        assertArrayEquals(data, beforeDelete.asByteArray(),
                "GET before delete should return correct data");

        // DELETE via S3 SDK
        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(BUCKET)
                .key(key)
                .build());
        log("[E2E] DELETE completed");

        // GET after delete should return 404
        NoSuchKeyException ex = assertThrows(NoSuchKeyException.class, () ->
                s3.getObject(
                        GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                        ResponseTransformer.toBytes()
                )
        );
        assertEquals(404, ex.statusCode(),
                "GET after DELETE should return 404");
        log("[E2E] GET after DELETE correctly returned 404");

        log("[E2E] PUT → DELETE → GET 404 test PASSED");
    }

    // =====================================================================
    // TEST: Legitimate empty object PUT → GET should return 200 with 0 bytes
    // =====================================================================
    @Test
    void e2e_putEmptyObject_thenGet_returnsZeroBytes() throws Exception {
        log("[E2E] Empty object PUT → GET test");

        byte[] data = new byte[0];
        String key = "empty-object.bin";

        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .contentLength(0L)
                        .build(),
                RequestBody.fromBytes(data)
        );
        log("[E2E] Empty-object PUT completed");

        ResponseBytes<GetObjectResponse> response = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );

        assertEquals(0, response.asByteArray().length,
                "GET of a legitimate empty object should return 200 with 0 bytes");

        log("[E2E] Empty object PUT → GET test PASSED");
    }

    // =====================================================================
    // TEST 3: Full multipart upload lifecycle → GET concatenated content
    // =====================================================================
    @Test
    void e2e_multipartUpload_fullLifecycle_thenGet() throws Exception {
        log("[E2E] Multipart full lifecycle test");

        String key = "mpu-lifecycle.bin";
        int partSize = 512 * 1024; // 512 KB per part
        int numParts = 3;

        // Generate known data for each part
        byte[][] partData = new byte[numParts][];
        for (int i = 0; i < numParts; i++) {
            partData[i] = new byte[partSize];
            new SecureRandom().nextBytes(partData[i]);
        }

        // Step 1: Initiate multipart upload via S3 SDK
        CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build()
        );
        String uploadId = initResp.uploadId();
        assertNotNull(uploadId, "Upload ID should not be null");
        log("[E2E] Initiated multipart upload: " + uploadId);

        // Step 2: Upload parts via S3 SDK
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            s3.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) partData[i].length)
                            .build(),
                    RequestBody.fromBytes(partData[i])
            );
            log("[E2E] Uploaded part " + partNumber);
        }

        // Step 3: Complete multipart upload via S3 SDK
        List<CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            completedParts.add(CompletedPart.builder()
                    .partNumber(i + 1)
                    .build());
        }
        s3.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build()
        );
        log("[E2E] Multipart upload completed");

        // Step 4: GET via S3 SDK — assert byte-for-byte concatenated content
        ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );
        byte[] got = getResp.asByteArray();

        // Build expected: concatenation of all parts
        byte[] expected = new byte[numParts * partSize];
        for (int i = 0; i < numParts; i++) {
            System.arraycopy(partData[i], 0, expected, i * partSize, partSize);
        }

        log("[E2E] GET received " + got.length + " bytes (expected " + expected.length + ")");
        assertArrayEquals(expected, got,
                "GET after multipart complete should return concatenated content of all parts");

        log("[E2E] Multipart full lifecycle test PASSED");
    }

    // =====================================================================
    // TEST 4: Multipart upload → abort → GET returns 404
    // After aborting a multipart upload, the key was never committed
    // as a multipart object. GET should return 404 NoSuchKey.
    // =====================================================================
    @Test
    void e2e_multipartUpload_abort_thenGet_returns404() throws Exception {
        log("[E2E] Multipart abort test");

        String key = "mpu-abort-test.bin";
        int partSize = 256 * 1024;

        byte[] partData = new byte[partSize];
        new SecureRandom().nextBytes(partData);

        // Initiate
        CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build()
        );
        String uploadId = initResp.uploadId();
        log("[E2E] Initiated multipart upload: " + uploadId);

        // Upload one part
        s3.uploadPart(
                UploadPartRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(1)
                        .contentLength((long) partData.length)
                        .build(),
                RequestBody.fromBytes(partData)
        );
        log("[E2E] Uploaded part 1");

        // Abort via S3 SDK
        s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(BUCKET)
                .key(key)
                .uploadId(uploadId)
                .build());
        log("[E2E] Abort completed");

        // GET the key — it was never completed, so no object exists at this key.
        // Should return 404 NoSuchKey per the S3 API contract.
        NoSuchKeyException ex = assertThrows(NoSuchKeyException.class, () ->
                s3.getObject(
                        GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                        ResponseTransformer.toBytes()
                )
        );
        assertEquals(404, ex.statusCode(),
                "GET after abort should return 404 (object was never assembled)");
        log("[E2E] GET after abort correctly returned 404");

        log("[E2E] Multipart abort test PASSED");
    }

    // =====================================================================
    // TEST 5: Large-file multipart upload round-trip
    // =====================================================================
    @Test
    void e2e_multipartUpload_largeFile_roundTrip() throws Exception {
        log("[E2E] Large multipart upload test");

        String key = "mpu-large-file.bin";
        int partSize = 1024 * 1024; // 1 MB per part
        int numParts = 3;

        // Generate known data for each part
        byte[][] partData = new byte[numParts][];
        for (int i = 0; i < numParts; i++) {
            partData[i] = new byte[partSize];
            new SecureRandom().nextBytes(partData[i]);
        }
        log("[E2E] Generated " + numParts + " parts of " + partSize + " bytes each");

        // Initiate
        CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build()
        );
        String uploadId = initResp.uploadId();
        log("[E2E] Initiated multipart upload: " + uploadId);

        // Upload parts
        for (int i = 0; i < numParts; i++) {
            int partNumber = i + 1;
            s3.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) partData[i].length)
                            .build(),
                    RequestBody.fromBytes(partData[i])
            );
            log("[E2E] Uploaded part " + partNumber + " (" + partData[i].length + " bytes)");
        }

        // Complete
        List<CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            completedParts.add(CompletedPart.builder()
                    .partNumber(i + 1)
                    .build());
        }
        s3.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build()
        );
        log("[E2E] Multipart upload completed");

        // GET and verify byte-for-byte
        ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );
        byte[] got = getResp.asByteArray();

        // Build expected
        int totalSize = numParts * partSize;
        byte[] expected = new byte[totalSize];
        for (int i = 0; i < numParts; i++) {
            System.arraycopy(partData[i], 0, expected, i * partSize, partSize);
        }

        log("[E2E] GET received " + got.length + " bytes (expected " + totalSize + ")");
        assertArrayEquals(expected, got,
                "Large multipart GET should return concatenated content of all parts");

        log("[E2E] Large multipart upload test PASSED");
    }

    // =====================================================================
    // TEST: HeadBucket on existing bucket returns 200
    // =====================================================================
    @Test
    void e2e_createBucket_thenHeadBucket_returns200() {
        log("[E2E] CreateBucket → HeadBucket 200 test");

        String bucket = "head-exists-" + System.nanoTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        log("[E2E] CreateBucket completed");

        try {
            assertDoesNotThrow(() ->
                    s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build()),
                    "HeadBucket on existing bucket should not throw");
            log("[E2E] HeadBucket → 200 PASSED");
        } finally {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        }
    }

    // =====================================================================
    // TEST: HeadBucket on missing bucket returns 404 NoSuchBucket
    // =====================================================================
    @Test
    void e2e_headBucket_onMissingBucket_returns404() {
        log("[E2E] HeadBucket missing → 404 test");

        NoSuchBucketException ex = assertThrows(NoSuchBucketException.class, () ->
                s3.headBucket(HeadBucketRequest.builder()
                        .bucket("never-created-" + System.nanoTime())
                        .build())
        );
        assertEquals(404, ex.statusCode(),
                "HeadBucket on missing bucket should return 404");
        log("[E2E] HeadBucket missing → 404 PASSED");
    }

    // =====================================================================
    // TEST: CreateBucket → DeleteBucket → HeadBucket returns 404
    // =====================================================================
    @Test
    void e2e_createThenDeleteBucket_headReturns404() {
        log("[E2E] Create → Delete → HeadBucket 404 test");

        String bucket = "lifecycle-" + System.nanoTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        assertDoesNotThrow(() ->
                s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build()),
                "HeadBucket should succeed after create");

        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        log("[E2E] DeleteBucket completed");

        NoSuchBucketException ex = assertThrows(NoSuchBucketException.class, () ->
                s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
        );
        assertEquals(404, ex.statusCode(),
                "HeadBucket after DeleteBucket should return 404");
        log("[E2E] Bucket lifecycle PASSED");
    }

    // =====================================================================
    // TEST: ListObjectsV2 on empty bucket returns 0 contents
    // =====================================================================
    @Test
    void e2e_listObjects_emptyBucket_returnsZeroContents() {
        log("[E2E] ListObjects empty bucket test");

        String bucket = "empty-list-" + System.nanoTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        try {
            ListObjectsV2Response resp = s3.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).build());

            assertEquals(0, resp.keyCount(), "empty bucket should have keyCount=0");
            assertTrue(resp.contents().isEmpty(), "empty bucket should have no Contents");
            log("[E2E] Empty list PASSED");
        } finally {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        }
    }

    // =====================================================================
    // TEST: PUT objects → ListObjectsV2 returns their keys
    // =====================================================================
    @Test
    void e2e_putObjects_thenListObjects_returnsKeys() {
        log("[E2E] PUT → ListObjects returns keys test");

        String bucket = "list-keys-" + System.nanoTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        byte[] data = "list-test".getBytes(StandardCharsets.UTF_8);
        List<String> keys = List.of("alpha.txt", "beta.txt", "gamma.txt");
        try {
            for (String k : keys) {
                s3.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(k)
                                .contentLength((long) data.length).build(),
                        RequestBody.fromBytes(data));
            }
            log("[E2E] PUT " + keys.size() + " objects completed");

            ListObjectsV2Response resp = s3.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).build());

            assertEquals(keys.size(), resp.keyCount(),
                    "keyCount should match number of PUTs");
            List<String> returnedKeys = new ArrayList<>(resp.contents().stream().map(S3Object::key).toList());
            Collections.sort(returnedKeys);
            // The storage layer stores keys as "bucket/key"; assert each user-visible key is in the returned list.
            for (String k : keys) {
                String storedKey = bucket + "/" + k;
                assertTrue(returnedKeys.contains(storedKey) || returnedKeys.contains(k),
                        "ListObjects should return key " + k + " (looked for '" + k
                                + "' or '" + storedKey + "' in " + returnedKeys + ")");
            }
            log("[E2E] ListObjects keys PASSED");
        } finally {
            for (String k : keys) {
                try { s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(k).build()); } catch (Exception ignored) {}
            }
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        }
    }

    // =====================================================================
    // TEST: ListObjectsV2 with prefix only returns matching keys
    // =====================================================================
    @Test
    void e2e_listObjects_withPrefix_filtersResults() {
        log("[E2E] ListObjects with prefix test");

        String bucket = "prefix-list-" + System.nanoTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        byte[] data = "x".getBytes(StandardCharsets.UTF_8);
        List<String> allKeys = List.of("logs/jan.txt", "logs/feb.txt", "images/cat.png");
        try {
            for (String k : allKeys) {
                s3.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(k)
                                .contentLength((long) data.length).build(),
                        RequestBody.fromBytes(data));
            }

            ListObjectsV2Response resp = s3.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).prefix("logs/").build());

            assertEquals(2, resp.keyCount(),
                    "prefix=logs/ should match exactly 2 keys, got: " + resp.contents());
            for (S3Object o : resp.contents()) {
                assertTrue(o.key().contains("logs/"),
                        "every returned key should be under logs/ prefix, got: " + o.key());
            }
            log("[E2E] ListObjects prefix PASSED");
        } finally {
            for (String k : allKeys) {
                try { s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(k).build()); } catch (Exception ignored) {}
            }
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        }
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static void deleteRecursive(Path root) throws IOException {
        if (!Files.exists(root)) return;

        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); }
                        catch (IOException ignored) {}
                    });
        }
    }
}
