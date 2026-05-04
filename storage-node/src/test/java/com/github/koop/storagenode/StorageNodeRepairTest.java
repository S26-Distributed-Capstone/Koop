package com.github.koop.storagenode;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.ErasureRouting;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
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
    private RocksDbRepairQueue repairQueue;
    private MetadataClient metadataClient;
    private MemoryFetcher fetcher;
    private PubSubClient pubSubClient;
    private MemoryPubSub pubSub;
    private Javalin ackServer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
        repairQueue = new RocksDbRepairQueue(strategy);
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
        if (metadataClient != null) metadataClient.close();
        if (pubSubClient != null) pubSubClient.close();
        if (http != null) http.close();
    }

    @Test
    void testNodeServesTrafficImmediatelyAfterStart() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();

        // Deliver a commit message with no prior PUT — blob is not on disk
        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "absent-key", "req-absent", getDummySender()), 200L);

        assertEquals(1, server.repairPendingCount(),
                "A repair should be enqueued when the commit arrives before the blob");
    }

    @Test
    void testNoRepairEnqueuedWhenBlobAlreadyMaterialized() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();

        server.processSequencerMessage(6,
                new Message.DeleteMessage("del-bucket", "del-key", "req-del", getDummySender()), 10L);

        assertEquals(0, server.repairPendingCount(),
                "Delete messages should not trigger repairs");
    }

    @Test
    void testCreateBucketMessageDoesNotEnqueueRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
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

    // -------------------------------------------------------------------------
    // Full E2E Repair Retrieval Integration
    // -------------------------------------------------------------------------

    @Test
    void testActualBlobRepair() throws Exception {
        // Setup original data and generate shards
        byte[] originalData = "Testing actual blob repair with erasure coding across nodes.".getBytes(StandardCharsets.UTF_8);
        int k = 2;
        int n = 3;
        InputStream[] shards;
        try (var bais = new java.io.ByteArrayInputStream(originalData)) {
            shards = ErasureCoder.shard(bais, originalData.length, k, n);
        }
        byte[] shard0 = shards[0].readAllBytes();
        byte[] shard1 = shards[1].readAllBytes();
        byte[] shard2 = shards[2].readAllBytes();

        // Setup mocked peer nodes
        Javalin peer1 = Javalin.create(config ->{
                config.startup.showJavalinBanner = false;
                config.routes.get("/store/{partition}/<blobKey>", ctx -> {
                    ctx.header("X-Koop-Type", "BLOB");
                    ctx.result(shard1);
                });
        }).start(0);
        Javalin peer2 = Javalin.create(config ->{
                config.startup.showJavalinBanner = false;
                config.routes.get("/store/{partition}/<blobKey>", ctx -> {
                    ctx.header("X-Koop-Type", "BLOB");
                    ctx.result(shard2);
                });
        }).start(0);

        try {
            // Start node under test
            server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
            server.start();
            port = server.port();

            // Provide populated metadata once the server port has been determined
            var es = new ErasureSetConfiguration.ErasureSet();
            es.setNumber(1);
            es.setK(k);
            es.setN(n);
            var m0 = new ErasureSetConfiguration.Machine(); m0.setIp("127.0.0.1"); m0.setPort(port);
            var m1 = new ErasureSetConfiguration.Machine(); m1.setIp("127.0.0.1"); m1.setPort(peer1.port());
            var m2 = new ErasureSetConfiguration.Machine(); m2.setIp("127.0.0.1"); m2.setPort(peer2.port());
            es.setMachines(List.of(m0, m1, m2));
            var esConfig = new ErasureSetConfiguration();
            esConfig.setErasureSets(List.of(es));

            var ps = new PartitionSpreadConfiguration.PartitionSpread();
            ps.setErasureSet(1);
            List<Integer> parts = new java.util.ArrayList<>();
            for(int i = 0; i < 1024; i++) parts.add(i);
            ps.setPartitions(parts);
            var psConfig = new PartitionSpreadConfiguration();
            psConfig.setPartitionSpread(List.of(ps));

            fetcher.update(esConfig);
            fetcher.update(psConfig);

            // Brief wait for metadata listener to recalculate node identities and subscriptions
            Thread.sleep(500);

            // Establish request context
            String bucket = "repair-bucket";
            String key = "repair-key-actual";
            String fullKey = bucket + "/" + key;
            String reqId = "req-repair-actual";

            ErasureRouting routing = new ErasureRouting(psConfig, esConfig);
            int partition = routing.getPartition(fullKey).orElseThrow();

            // Trigger COMMIT_MISS by processing sequencer message directly, without issuing PUT beforehand
            server.processSequencerMessage(partition, new Message.FileCommitMessage(bucket, key, reqId, getDummySender()), 100L);

            // Verify completion by polling for a successful object payload
            boolean repaired = false;
            for (int i = 0; i < 50; i++) {
                if (server.repairPendingCount() == 0) {
                    HttpRequest getReq = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey))
                            .GET()
                            .build();
                    HttpResponse<byte[]> res = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
                    if (res.statusCode() == 200) {
                        assertArrayEquals(shard0, res.body(), "Recovered shard should match original shard0 data");
                        repaired = true;
                        break;
                    }
                }
                Thread.sleep(100);
            }
            assertTrue(repaired, "Node should have repaired the blob successfully by fetching and reconstructing from peers");

        } finally {
            peer1.stop();
            peer2.stop();
        }
    }
}