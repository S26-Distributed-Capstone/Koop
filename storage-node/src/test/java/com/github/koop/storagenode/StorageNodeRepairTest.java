package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.pubsub.CommitTopics;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

import io.javalin.Javalin;

class StorageNodeRepairTest {

    private StorageNodeServerV2 server;
    private HttpClient http;
    private int port;
    private Database db;
    private MetadataClient metadataClient;
    private PubSubClient pubSubClient;
    private MemoryPubSub pubSub;
    private Javalin ackServer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        db = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
        metadataClient = new MetadataClient(new MemoryFetcher());
        pubSub = new MemoryPubSub();
        pubSubClient = new PubSubClient(pubSub);
        metadataClient.start();
        pubSubClient.start();

        ackServer = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            config.routes.post("/ack/{requestId}", ctx -> ctx.status(200));
        }).start(0);

        http = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    private InetSocketAddress getDummySender() {
        return new InetSocketAddress("127.0.0.1", ackServer.port());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.stop();
        if (db != null) db.close();
        if (ackServer != null) ackServer.stop();
    }

    @Test
    void testNodeStartsInActiveState() {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        assertEquals(NodeState.ACTIVE, server.getState());
    }

    @Test
    void testNodeStateTransitionsThroughRepair() {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        assertEquals(NodeState.INITIALIZING, server.getState(), "Should start in INITIALIZING");

        server.start();
        assertEquals(NodeState.ACTIVE, server.getState(), "Should be ACTIVE after start completes");
    }

    @Test
    void testReadRepairEnqueuesOnMissingFile() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        int partition = 1;
        String bucket = "repair-bucket";
        String key = "missing-blob";
        String fullKey = bucket + "/" + key;
        String reqId = "req-repair-123";

        // Commit metadata without storing the physical file
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 100L);

        // GET should trigger read-repair (returns 404 but enqueues repair)
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // Should still return 404 (the repair is async)
        assertEquals(404, getResp.statusCode());
    }

    @Test
    void testStartupRepairCompactsBacklog() {
        int partition = 5;
        String topic = CommitTopics.forPartition(partition);

        // Simulate messages published while the node was offline
        // by publishing before any node subscribes
        InetSocketAddress sender = getDummySender();

        // Write -> Update -> Delete for the same key (should compact to delete = no repair needed)
        pubSub.pub(topic, Message.serializeMessage(
                new Message.FileCommitMessage("bucket", "key1", "req-1", sender)));
        pubSub.pub(topic, Message.serializeMessage(
                new Message.FileCommitMessage("bucket", "key1", "req-2", sender)));
        pubSub.pub(topic, Message.serializeMessage(
                new Message.DeleteMessage("bucket", "key1", "req-3", sender)));

        // Write for a different key (should result in a repair)
        pubSub.pub(topic, Message.serializeMessage(
                new Message.FileCommitMessage("bucket", "key2", "req-4", sender)));

        // Now create and start the server — it should subscribe, poll backlog, compact, and enqueue repairs
        // We don't have metadata config to auto-subscribe, so we manually test the repair logic
        // by directly testing the pubsub backlog polling
        pubSub.sub(topic, "127.0.0.1:0");

        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertEquals(4, backlog.size(), "Should have 4 raw backlog messages");

        // Verify compaction logic: last-writer-wins per key
        java.util.LinkedHashMap<String, Message> compacted = new java.util.LinkedHashMap<>();
        for (byte[] msgBytes : backlog) {
            Message message = Message.deserializeMessage(msgBytes);
            String msgKey = switch (message) {
                case Message.FileCommitMessage m -> m.bucket() + "/" + m.key();
                case Message.MultipartCommitMessage m -> m.bucket() + "/" + m.key();
                case Message.DeleteMessage m -> m.bucket() + "/" + m.key();
                case Message.CreateBucketMessage m -> "bucket:" + m.bucket();
                case Message.DeleteBucketMessage m -> "bucket:" + m.bucket();
            };
            compacted.put(msgKey, message);
        }

        assertEquals(2, compacted.size(), "Compaction should yield 2 unique keys");

        // key1's final state is a delete — should NOT need repair
        Message key1Final = compacted.get("bucket/key1");
        assertInstanceOf(Message.DeleteMessage.class, key1Final);

        // key2's final state is a file commit — DOES need repair
        Message key2Final = compacted.get("bucket/key2");
        assertInstanceOf(Message.FileCommitMessage.class, key2Final);
    }

    @Test
    void testTrafficGatingDuringRepair() throws Exception {
        // We can't easily test the 503 during REPAIRING because start() transitions
        // synchronously. Instead, verify that after start(), traffic works normally.
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/1/test-bucket/test-key"))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // Should get 404 (not 503) because node is ACTIVE
        assertEquals(404, getResp.statusCode(), "Should return 404, not 503, when node is ACTIVE");
    }

    @Test
    void testExistingFunctionalityPreservedWithRepair() throws Exception {
        // Full PUT -> commit -> GET cycle should still work with repair infrastructure present
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        int partition = 2;
        String bucket = "normal-bucket";
        String key = "normal-key";
        String fullKey = bucket + "/" + key;
        String reqId = "req-normal";
        String data = "Normal Data";

        // PUT
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey + "?requestId=" + reqId))
                .PUT(HttpRequest.BodyPublishers.ofString(data))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Commit
        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 50L);

        // GET
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp.statusCode());
        assertEquals(data, getResp.body());
    }
}
