package com.github.koop.storagenode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
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
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

import io.javalin.Javalin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageNodeServerV2Test {

    private StorageNodeServerV2 server;
    private HttpClient http;
    private int port;
    private Database db;
    private MetadataClient metadataClient;
    private PubSubClient pubSubClient;
    private MemoryFetcher fetcher;
    private MemoryPubSub pubSub;

    // A dummy server to absorb all the async ACKs and prevent connection errors in
    // the logs
    private Javalin ackServer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        db = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
        fetcher = new MemoryFetcher();
        metadataClient = new MetadataClient(fetcher);
        pubSub = new MemoryPubSub();
        pubSubClient = new PubSubClient(pubSub);
        // Start the ACK blackhole server on a random port
        ackServer = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            config.routes.post("/ack", ctx -> ctx.status(200));
        }).start(0);

        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
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

    private URI storeUri(int partition, String key) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + encodedKey);
    }

    private URI storeUriWithReq(int partition, String key, String reqId) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI
                .create("http://localhost:" + port + "/store/" + partition + "/" + encodedKey + "?requestId=" + reqId);
    }

    private URI storeUriWithVersion(int partition, String key, long version) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI
                .create("http://localhost:" + port + "/store/" + partition + "/" + encodedKey + "?version=" + version);
    }

    @Test
    void testPutMissingRequestId() throws Exception {
        int partition = 1;
        String key = "missing-reqid";

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key)) // Omitting ?requestId=
                .PUT(HttpRequest.BodyPublishers.ofString("Data"))
                .build();

        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, putResp.statusCode(), "PUT should return 400 when requestId is missing");
        assertEquals("Missing requestId parameter", putResp.body());
    }

    @Test
    void testGetInvalidVersion() throws Exception {
        int partition = 1;
        String key = "invalid-version-key";

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + key + "?version=abc"))
                .GET()
                .build();

        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, getResp.statusCode(), "GET should return 400 when version is invalid");
        assertEquals("Invalid version parameter", getResp.body());
    }

    @Test
    void testPutUncommittedAndGetNotFound() throws Exception {
        int partition = 1;
        String key = "test-key";
        byte[] data = "Hello".getBytes(StandardCharsets.UTF_8);

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "req-1"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .header("Content-Type", "application/octet-stream")
                .build();

        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode(), "PUT should succeed");

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
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
        assertEquals("100", getResp.headers().firstValue("X-Object-Version").orElse(""));
    }

    @Test
    void testGetSpecificVersion() throws Exception {
        int partition = 3;
        String key = "versioned-key";

        HttpRequest putReq1 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "req-v1"))
                .PUT(HttpRequest.BodyPublishers.ofString("Version 1"))
                .build();
        http.send(putReq1, HttpResponse.BodyHandlers.ofString());
        db.putItem(key, partition, 10L, "req-v1");

        HttpRequest putReq2 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "req-v2"))
                .PUT(HttpRequest.BodyPublishers.ofString("Version 2"))
                .build();
        http.send(putReq2, HttpResponse.BodyHandlers.ofString());
        db.putItem(key, partition, 20L, "req-v2");

        HttpRequest getV1 = HttpRequest.newBuilder()
                .uri(storeUriWithVersion(partition, key, 10L))
                .GET()
                .build();
        HttpResponse<String> respV1 = http.send(getV1, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, respV1.statusCode());
        assertEquals("Version 1", respV1.body());

        HttpRequest getLatest = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
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
        assertEquals(404, getResp.statusCode());
        assertTrue(getResp.body().contains("Tombstone"));
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
        AtomicReference<String> receivedBody = new AtomicReference<>();

        // Start a dummy Javalin server on a random port to act as the sequencer
        // callback endpoint
        Javalin coordinator = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.post("/ack", ctx -> {
                receivedBody.set(ctx.body());
                ackLatch.countDown();
                ctx.status(200);
            });
        }).start(0);

        int callbackPort = coordinator.port();
        InetSocketAddress senderAddress = new InetSocketAddress("127.0.0.1", callbackPort);

        // Create a simple message to trigger the sequencer processing and subsequent
        // ACK
        Message.CreateBucketMessage msg = new Message.CreateBucketMessage(bucket, reqId, senderAddress);

        // Process the message (this handles the database operation and fires the async
        // ACK)
        server.processSequencerMessage(partition, msg, 1L);

        // Wait for the asynchronous HTTP client to send the request to the dummy server
        boolean received = ackLatch.await(5, TimeUnit.SECONDS);

        assertTrue(received, "ACK was not received by the callback server within the timeout limit");
        assertEquals("{\"requestId\":\"" + reqId + "\"}", receivedBody.get(),
                "ACK payload should contain the correct JSON formatting");

    }

    @Test
    void testGetCommittedButUnmaterializedRegularFile() throws Exception {
        int partition = 6;
        String bucket = "missing-bucket";
        String key = "missing-file";
        String fullKey = bucket + "/" + key;
        String reqId = "req-missing-123";

        // Commit the file metadata via sequencer message WITHOUT performing the HTTP PUT first.
        // This simulates a scenario where the DB commit arrives but the file isn't on disk.
        server.processSequencerMessage(partition, 
            new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 300L);

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // StorageNodeV2 checks Files.exists(path) for RegularFileVersion and returns Optional.empty(),
        // which the server translates to 404 NOT_FOUND.
        assertEquals(404, getResp.statusCode(), "GET should return 404 when file metadata exists but the physical file is missing from disk");
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
        assertEquals(200, getResp.statusCode(), "GET should return 200 for multipart manifest even if physical chunk files are unmaterialized");
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
        assertEquals("500", getResp2.headers().firstValue("X-Object-Version").orElse(""), 
            "The version headers should match the initial sequencer commit");
    }
}