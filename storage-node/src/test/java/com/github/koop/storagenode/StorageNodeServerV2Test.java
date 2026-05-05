package com.github.koop.storagenode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

import io.javalin.Javalin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class StorageNodeServerV2Test {

    private StorageNodeServerV2 server;
    private HttpClient http;
    private int port;
    private Database db;
    private MetadataClient metadataClient;
    private PubSubClient pubSubClient;
    private MemoryFetcher fetcher;
    private MemoryPubSub pubSub;

    // A dummy server to absorb all the async ACKs and prevent connection errors in the logs
    private Javalin ackServer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
        RocksDbRepairQueue repairQueue = new RocksDbRepairQueue(strategy);
        db = new Database(strategy);
        fetcher = new MemoryFetcher();
        metadataClient = new MetadataClient(fetcher);
        pubSub = new MemoryPubSub();
        pubSubClient = new PubSubClient(pubSub);
        metadataClient.start();
        pubSubClient.start();

        // Feed empty metadata so the waitFor completes immediately
        var emptyEs = new ErasureSetConfiguration();
        emptyEs.setErasureSets(List.of());
        var emptyPs = new PartitionSpreadConfiguration();
        emptyPs.setPartitionSpread(List.of());
        fetcher.update(emptyEs);
        fetcher.update(emptyPs);

        // Start the ACK blackhole server on a random port (Updated to new path param spec)
        ackServer = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            config.routes.post("/ack/{requestId}", ctx -> ctx.status(200));
        }).start(0);

        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();
        port = server.port();

        http = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    private InetSocketAddress getDummySender() {
        return new InetSocketAddress("127.0.0.1", ackServer.port());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
        db.close();
        ackServer.stop();
    }

    private URI storeUri(int partition, String fullKey) {
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey);
    }

    private URI storeUriWithReq(int partition, String fullKey, String reqId) {
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey + "?requestId=" + reqId);
    }

    private URI storeUriWithVersion(int partition, String fullKey, long version) {
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey + "?version=" + version);
    }

    @Test
    void testPutMissingRequestId() throws Exception {
        int partition = 1;
        String fullKey = "test-bucket/missing-reqid";

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey)) // Omitting ?requestId=
                .PUT(HttpRequest.BodyPublishers.ofString("Data"))
                .build();

        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, putResp.statusCode(), "PUT should return 400 when requestId is missing");
        assertEquals("Missing requestId parameter", putResp.body());
    }

    @Test
    void testGetInvalidVersion() throws Exception {
        int partition = 1;
        String fullKey = "test-bucket/invalid-version-key";

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey + "?version=abc"))
                .GET()
                .build();

        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, getResp.statusCode(), "GET should return 400 when version is invalid");
        assertEquals("Invalid version parameter", getResp.body());
    }

    @Test
    void testPutUncommittedAndGetNotFound() throws Exception {
        int partition = 1;
        String fullKey = "test-bucket/test-key";
        byte[] data = "Hello".getBytes(StandardCharsets.UTF_8);

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey, "req-1"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .header("Content-Type", "application/octet-stream")
                .build();

        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode(), "PUT should succeed");

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();

        HttpResponse<byte[]> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(404, getResp.statusCode(), "GET should return 404 since it is uncommitted");
    }

    @Test
    void testPutCommitAndGet() throws Exception {
        int partition = 2;
        String bucket = "my-bucket";
        String key = "test-key";
        String fullKey = bucket + "/" + key;
        String reqId = "req-commit";
        long seqNum = 100L;
        String dataStr = "Commit Data";

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey, reqId))
                .PUT(HttpRequest.BodyPublishers.ofString(dataStr))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        Message.FileCommitMessage commitMsg = new Message.FileCommitMessage(
                bucket, key, reqId, getDummySender());
        server.processSequencerMessage(partition, commitMsg, seqNum);

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp.statusCode());
        assertEquals(dataStr, getResp.body());
        assertEquals("100", getResp.headers().firstValue("X-Koop-Version").orElse(""));
    }

    @Test
    void testGetSpecificVersion() throws Exception {
        int partition = 3;
        String fullKey = "test-bucket/versioned-key";

        HttpRequest putReq1 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey, "req-v1"))
                .PUT(HttpRequest.BodyPublishers.ofString("Version 1"))
                .build();
        http.send(putReq1, HttpResponse.BodyHandlers.ofString());
        db.putItem(fullKey, partition, 10L, "req-v1");

        HttpRequest putReq2 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey, "req-v2"))
                .PUT(HttpRequest.BodyPublishers.ofString("Version 2"))
                .build();
        http.send(putReq2, HttpResponse.BodyHandlers.ofString());
        db.putItem(fullKey, partition, 20L, "req-v2");

        HttpRequest getV1 = HttpRequest.newBuilder()
                .uri(storeUriWithVersion(partition, fullKey, 10L))
                .GET()
                .build();
        HttpResponse<String> respV1 = http.send(getV1, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, respV1.statusCode());
        assertEquals("Version 1", respV1.body());

        HttpRequest getLatest = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> respLatest = http.send(getLatest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, respLatest.statusCode());
        assertEquals("Version 2", respLatest.body());
    }

    @Test
    void testProcessDeleteMessage() throws Exception {
        int partition = 4;
        String bucket = "my-bucket";
        String key = "del-key";
        String fullKey = bucket + "/" + key;
        String reqId = "req-del-put";

        // Store and commit an object
        http.send(HttpRequest.newBuilder().uri(storeUriWithReq(partition, fullKey, reqId))
                .PUT(HttpRequest.BodyPublishers.ofString("To Be Deleted")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 50L);

        // Delete object via sequencer
        server.processSequencerMessage(partition,
                new Message.DeleteMessage(bucket, key, "req-delete", getDummySender()), 51L);

        // Verify it returns 404 and Tombstone
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResp.statusCode());
        assertEquals("TOMBSTONE", getResp.headers().firstValue("X-Koop-Type").orElse(""));
    }

    @Test
    void testMultipartCommitMessageAndGet() throws Exception {
        int partition = 5;
        String bucket = "multi-bucket";
        String key = "multi-key";
        String fullKey = bucket + "/" + key;

        // Commit a multipart object
        List<String> chunks = List.of("chunk1", "chunk2");
        server.processSequencerMessage(partition,
                new Message.MultipartCommitMessage(bucket, key, "req-multi", getDummySender(),
                        chunks),
                200L);

        // Retrieve the multipart chunk list
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp.statusCode());
        assertEquals("application/json", getResp.headers().firstValue("Content-Type").orElse(""));
        assertTrue(getResp.body().contains("chunk1"));
        assertTrue(getResp.body().contains("chunk2"));
    }

    @Test
    void testProcessSequencerMessageSendsAck() throws Exception {
        int partition = 1;
        String bucket = "ack-bucket";
        String reqId = "req-ack-123";

        CountDownLatch ackLatch = new CountDownLatch(1);
        AtomicReference<String> receivedReqId = new AtomicReference<>();

        // Start a dummy Javalin server on a random port to act as the sequencer callback endpoint
        Javalin coordinator = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.post("/ack/{requestId}", ctx -> {
                receivedReqId.set(ctx.pathParam("requestId"));
                ackLatch.countDown();
                ctx.status(200);
            });
        }).start(0);

        int callbackPort = coordinator.port();
        InetSocketAddress senderAddress = new InetSocketAddress("127.0.0.1", callbackPort);

        // Create a simple message to trigger the sequencer processing and subsequent ACK
        Message.CreateBucketMessage msg = new Message.CreateBucketMessage(bucket, reqId, senderAddress);

        // Process the message (this handles the database operation and fires the async ACK)
        server.processSequencerMessage(partition, msg, 1L);

        // Wait for the asynchronous HTTP client to send the request to the dummy server
        boolean received = ackLatch.await(5, TimeUnit.SECONDS);

        assertTrue(received, "ACK was not received by the callback server within the timeout limit");
        assertEquals(reqId, receivedReqId.get(), "ACK should use path parameter for requestId");
        coordinator.stop();
    }

    @Test
    void testGetCommittedButUnmaterializedRegularFile() throws Exception {
        int partition = 6;
        String bucket = "missing-bucket";
        String key = "missing-file";
        String fullKey = bucket + "/" + key;
        String reqId = "req-missing-123";

        // Commit the file metadata via sequencer message WITHOUT performing the HTTP PUT first.
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 300L);

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // StorageNodeV2 checks Files.exists(path) for RegularFileVersion and returns Optional.empty(),
        // which the server translates to 404 NOT_FOUND.
        assertEquals(404, getResp.statusCode(),
                "GET should return 404 when file metadata exists but the physical file is missing from disk");
    }

    @Test
    void testGetCommittedButUnmaterializedMultipartManifest() throws Exception {
        int partition = 7;
        String bucket = "missing-multi-bucket";
        String key = "missing-multi";
        String fullKey = bucket + "/" + key;
        String reqId = "req-multi-missing";

        // Commit the multipart manifest WITHOUT putting the actual chunk files on disk.
        List<String> chunks = List.of("missing-chunk1", "missing-chunk2");
        server.processSequencerMessage(partition,
                new Message.MultipartCommitMessage(bucket, key, reqId, getDummySender(), chunks), 400L);

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // StorageNodeV2 DOES NOT verify physical existence of individual chunks during a manifest retrieval.
        // It returns the JSON list of chunks immediately.
        assertEquals(200, getResp.statusCode(),
                "GET should return 200 for multipart manifest even if physical chunk files are unmaterialized");
        assertEquals("application/json", getResp.headers().firstValue("Content-Type").orElse(""));
        assertTrue(getResp.body().contains("missing-chunk1"));
        assertTrue(getResp.body().contains("missing-chunk2"));
    }

    @Test
    void testPostCommitMaterialization() throws Exception {
        int partition = 8;
        String bucket = "post-commit-bucket";
        String key = "late-file";
        String fullKey = bucket + "/" + key;
        String reqId = "req-late-123";
        String dataStr = "Better late than never";

        // 1. Commit the file metadata via sequencer message FIRST (before the file is uploaded)
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 500L);

        // 2. Verify the file is not yet available (returns 404 because physical file is missing)
        HttpRequest getReq1 = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        assertEquals(404, http.send(getReq1, HttpResponse.BodyHandlers.ofString()).statusCode(),
                "GET should return 404 when committed but not yet materialized");

        // 3. Materialize the physical file on disk via HTTP PUT
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey, reqId))
                .PUT(HttpRequest.BodyPublishers.ofString(dataStr))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode(),
                "PUT should succeed in materializing the file");

        // 4. Verify the file is now successfully retrievable
        HttpRequest getReq2 = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp2 = http.send(getReq2, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp2.statusCode(), "GET should return 200 after the file is materialized");
        assertEquals(dataStr, getResp2.body(), "GET should return the correctly materialized data");
        assertEquals("500", getResp2.headers().firstValue("X-Koop-Version").orElse(""),
                "The version headers should match the initial sequencer commit");
    }

    // ─── Bucket query endpoint helpers ────────────────────────────────────────

    private URI bucketUri(String bucket) {
        return URI.create("http://localhost:" + port + "/bucket/" + bucket);
    }

    private URI bucketUriWithParams(String bucket, String prefix, Integer maxKeys) {
        StringBuilder sb = new StringBuilder("http://localhost:" + port + "/bucket/" + bucket);
        String sep = "?";
        if (prefix != null) { sb.append(sep).append("prefix=").append(prefix); sep = "&"; }
        if (maxKeys != null) { sb.append(sep).append("maxKeys=").append(maxKeys); }
        return URI.create(sb.toString());
    }

    // ─── HeadBucket tests ─────────────────────────────────────────────────────

    @Test
    void testHeadBucket_returnsNotFoundWhenBucketDoesNotExist() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri("no-such-bucket"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        assertEquals(404, resp.statusCode(), "HEAD should return 404 for non-existent bucket");
    }

    @Test
    void testHeadBucket_returnsOkAfterBucketCreated() throws Exception {
        int partition = 10;
        String bucket = "head-test-bucket";

        // Create bucket via sequencer message
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-head-create", getDummySender()), 600L);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, resp.statusCode(), "HEAD should return 200 after bucket is created");
    }

    @Test
    void testHeadBucket_returnsNotFoundAfterBucketDeleted() throws Exception {
        int partition = 11;
        String bucket = "head-delete-bucket";

        // Create then delete
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-hd-create", getDummySender()), 700L);
        server.processSequencerMessage(partition,
                new Message.DeleteBucketMessage(bucket, "req-hd-delete", getDummySender()), 701L);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        assertEquals(404, resp.statusCode(), "HEAD should return 404 after bucket is deleted");
    }

    // ─── ListObjects tests ────────────────────────────────────────────────────

    @Test
    void testListObjects_returnsEmptyListWhenNoObjects() throws Exception {
        int partition = 12;
        String bucket = "empty-list-bucket";

        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-empty-list", getDummySender()), 800L);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertEquals("application/json", resp.headers().firstValue("Content-Type").orElse(""));
        assertEquals("[]", resp.body().trim());
    }

    @Test
    void testListObjects_returnsObjectsInBucket() throws Exception {
        int partition = 13;
        String bucket = "list-bucket";
        String key1 = "file1.txt";
        String key2 = "file2.txt";
        String fullKey1 = bucket + "/" + key1;
        String fullKey2 = bucket + "/" + key2;

        // Create bucket first so that GET /bucket/{bucket} doesn't 404
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-list-create", getDummySender()), 899L);

        // PUT + commit two objects
        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey1, "req-list-1"))
                .PUT(HttpRequest.BodyPublishers.ofString("data1")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key1, "req-list-1", getDummySender()), 900L);

        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey2, "req-list-2"))
                .PUT(HttpRequest.BodyPublishers.ofString("data2")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key2, "req-list-2", getDummySender()), 901L);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains(fullKey1), "Response should contain " + fullKey1);
        assertTrue(resp.body().contains(fullKey2), "Response should contain " + fullKey2);
    }

    @Test
    void testListObjects_respectsMaxKeysParam() throws Exception {
        int partition = 14;
        String bucket = "maxkeys-bucket";

        // Create bucket first
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-mk-create", getDummySender()), 999L);

        // Commit three objects
        for (int i = 1; i <= 3; i++) {
            String key = "obj" + i;
            String fullKey = bucket + "/" + key;
            String reqId = "req-mk-" + i;
            http.send(HttpRequest.newBuilder()
                    .uri(storeUriWithReq(partition, fullKey, reqId))
                    .PUT(HttpRequest.BodyPublishers.ofString("data" + i)).build(),
                    HttpResponse.BodyHandlers.ofString());
            server.processSequencerMessage(partition,
                    new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 1000L + i);
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUriWithParams(bucket, null, 1))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());

        // Count how many "key" entries appear. With maxKeys=1, should be exactly 1 object.
        // Simple check: split by "key" field separator
        String body = resp.body();
        long objectCount = body.chars().filter(ch -> ch == '{').count();
        assertEquals(1, objectCount, "maxKeys=1 should return exactly 1 object, got: " + body);
    }

    @Test
    void testListObjects_onlyReturnsObjectsMatchingPrefix() throws Exception {
        int partition = 15;
        String bucket = "prefix-bucket";

        // Create both buckets first
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-pf-create", getDummySender()), 1099L);
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage("other-bucket", "req-pf-other-create", getDummySender()), 1098L);

        // Two objects in "prefix-bucket", one in "other-bucket"
        String fullKey1 = bucket + "/alpha";
        String fullKey2 = bucket + "/beta";
        String otherKey = "other-bucket/gamma";

        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey1, "req-pf-1"))
                .PUT(HttpRequest.BodyPublishers.ofString("a")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, "alpha", "req-pf-1", getDummySender()), 1100L);

        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey2, "req-pf-2"))
                .PUT(HttpRequest.BodyPublishers.ofString("b")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, "beta", "req-pf-2", getDummySender()), 1101L);

        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, otherKey, "req-pf-3"))
                .PUT(HttpRequest.BodyPublishers.ofString("c")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage("other-bucket", "gamma", "req-pf-3", getDummySender()), 1102L);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains(fullKey1));
        assertTrue(resp.body().contains(fullKey2));
        assertFalse(resp.body().contains(otherKey), "Should not include objects from other buckets");
    }

    @Test
    void testListObjects_excludesDeletedObjects() throws Exception {
        int partition = 16;
        String bucket = "tombstone-list-bucket";
        String key1 = "alive.txt";
        String key2 = "deleted.txt";
        String fullKey1 = bucket + "/" + key1;
        String fullKey2 = bucket + "/" + key2;

        // Create bucket
        server.processSequencerMessage(partition,
                new Message.CreateBucketMessage(bucket, "req-ts-create", getDummySender()), 1200L);

        // PUT + commit both objects
        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey1, "req-ts-1"))
                .PUT(HttpRequest.BodyPublishers.ofString("data1")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key1, "req-ts-1", getDummySender()), 1201L);

        http.send(HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, fullKey2, "req-ts-2"))
                .PUT(HttpRequest.BodyPublishers.ofString("data2")).build(),
                HttpResponse.BodyHandlers.ofString());
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key2, "req-ts-2", getDummySender()), 1202L);

        // Delete the second object (writes a tombstone)
        server.processSequencerMessage(partition,
                new Message.DeleteMessage(bucket, key2, "req-ts-del", getDummySender()), 1203L);

        // List objects — deleted key should NOT appear
        HttpRequest req = HttpRequest.newBuilder()
                .uri(bucketUri(bucket))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains(fullKey1), "Living object should still appear in listing");
        assertFalse(resp.body().contains(fullKey2),
                "Deleted (tombstoned) object should NOT appear in listing, but got: " + resp.body());
    }
}