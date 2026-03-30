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

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
        db.close();
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
                bucket, key, reqId, new InetSocketAddress("127.0.0.1", 8080));
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
                new Message.FileCommitMessage(bucket, key, reqId, new InetSocketAddress("127.0.0.1", 8080)), 50L);

        // Delete object via sequencer
        server.processSequencerMessage(partition,
                new Message.DeleteMessage(bucket, key, "req-delete", new InetSocketAddress("127.0.0.1", 8080)), 51L);

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
                new Message.MultipartCommitMessage(bucket, key, "req-multi", new InetSocketAddress("127.0.0.1", 8080),
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
}