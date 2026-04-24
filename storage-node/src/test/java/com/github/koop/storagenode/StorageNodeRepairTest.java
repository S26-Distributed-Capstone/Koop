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
    void testNodeServesTrafficImmediatelyAfterStart() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/1/any-bucket/any-key"))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        // Node serves traffic immediately — no 503 gating
        assertNotEquals(503, resp.statusCode(), "Node should not return 503 after start");
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
                "A repair should be enqueued when the commit arrives before the blob");
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
    // GET on committed-but-missing blob returns 404 (no read-repair)
    // -------------------------------------------------------------------------

    @Test
    void testGetOnMissingBlobReturns404WithoutEnqueueingRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();
        port = server.port();

        // Commit without PUT — metadata exists but file is not on disk
        // This also enqueues a COMMIT_MISS repair (count = 1)
        server.processSequencerMessage(5,
                new Message.FileCommitMessage("rb", "ghost", "req-ghost", getDummySender()), 400L);

        int repairCountAfterCommit = server.repairPendingCount();

        // GET should return 404 but NOT enqueue additional repair (read-repair removed)
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/5/rb/ghost"))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, resp.statusCode());
        // Repair count should not increase from the GET — only the commit path enqueues
        assertEquals(repairCountAfterCommit, server.repairPendingCount(),
                "GET should not enqueue additional repair; only the commit path triggers repair");
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

    // -------------------------------------------------------------------------
    // Sequence number compaction in the repair queue
    // -------------------------------------------------------------------------

    @Test
    void testNewerCommitMissOverridesOlderForSameKey() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient);
        server.start();

        // Two commits for the same key — second has higher seqNumber
        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "dup-key", "req-dup1", getDummySender()), 100L);
        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "dup-key", "req-dup2", getDummySender()), 200L);

        // Should compact to 1 pending repair (latest seqNumber wins)
        assertEquals(1, server.repairPendingCount(),
                "Multiple commits for the same key should compact to 1 pending repair");
    }
}
