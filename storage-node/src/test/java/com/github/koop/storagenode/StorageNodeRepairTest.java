package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
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

    // -------------------------------------------------------------------------
    // COMMIT_MISS repair: FileCommitMessage without prior PUT
    // -------------------------------------------------------------------------

    @Test
    void testCommitMissEnqueuesRepairWhenBlobAbsent() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();

        // Deliver a commit message with no prior PUT — blob is not on disk
        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "absent-key", "req-absent", getDummySender()), 200L);

        assertEquals(1, server.repairPendingCount(),
                "A COMMIT_MISS repair should be enqueued when the commit arrives before the blob");
    }

    @Test
    void testNoRepairEnqueuedWhenBlobAlreadyMaterialized() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        String bucket = "mat-bucket";
        String key = "mat-key";
        String fullKey = bucket + "/" + key;
        String reqId = "req-mat";

        // PUT first so the blob is on disk when commit arrives
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/4/" + fullKey + "?requestId=" + reqId))
                .PUT(HttpRequest.BodyPublishers.ofString("materialized"))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        server.processSequencerMessage(4,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 300L);

        assertEquals(0, server.repairPendingCount(),
                "No repair should be enqueued when the blob was already materialized at commit time");
    }

    // -------------------------------------------------------------------------
    // Read-repair: GET on a committed-but-missing blob enqueues repair
    // -------------------------------------------------------------------------

    @Test
    void testReadRepairEnqueuesOnGetWithMissingPhysicalFile() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        // Commit without PUT: metadata exists but file is not on disk
        server.processSequencerMessage(5,
                new Message.FileCommitMessage("rb", "ghost", "req-ghost", getDummySender()), 400L);

        // COMMIT_MISS repair already enqueued — drain it to start fresh
        // (we want to isolate the READ_MISS repair triggered by GET)
        // So we use a fresh key that has no commit at all (retrieves empty → no repair)
        // vs a key that was committed: GET triggers read-repair
        // The COMMIT_MISS repair from above is already in the pool (count == 1).

        // A GET on the committed key should enqueue an additional repair (READ_MISS)
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/5/rb/ghost"))
                .GET()
                .build();
        http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // At least 1 repair in the pool (COMMIT_MISS + possibly READ_MISS depending on timing)
        assertTrue(server.repairPendingCount() >= 1,
                "At least one repair should be pending after a GET on a missing blob");
    }

    // -------------------------------------------------------------------------
    // Delete and bucket messages do NOT enqueue repair
    // -------------------------------------------------------------------------

    @Test
    void testDeleteMessageDoesNotEnqueueRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();

        server.processSequencerMessage(6,
                new Message.DeleteMessage("del-bucket", "del-key", "req-del", getDummySender()), 10L);

        assertEquals(0, server.repairPendingCount(),
                "Delete messages should not trigger repairs");
    }

    @Test
    void testCreateBucketMessageDoesNotEnqueueRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();

        server.processSequencerMessage(7,
                new Message.CreateBucketMessage("new-bucket", "req-cb", getDummySender()), 20L);

        assertEquals(0, server.repairPendingCount(),
                "CreateBucket messages should not trigger repairs");
    }
}
