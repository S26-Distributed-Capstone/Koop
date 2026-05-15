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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

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

        assertNotEquals(503, resp.statusCode(), "Node should not return 503 after start");
    }

    @Test
    void testExistingFunctionalityPreservedWithRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();
        port = server.port();

        int partition = 2;
        String bucket = "normal-bucket";
        String key = "normal-key";
        String fullKey = bucket + "/" + key;
        String reqId = "req-normal";
        String data = "Normal Data";

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey + "?requestId=" + reqId))
                .PUT(HttpRequest.BodyPublishers.ofString(data))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        server.processSequencerMessage(partition,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender(), 1024L), 50L);

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey))
                .GET()
                .build();
        HttpResponse<String> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp.statusCode());
        assertEquals(data, getResp.body());
    }

    @Test
    void testCommitMissEnqueuesRepairWhenBlobAbsent() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();

        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "absent-key", "req-absent", getDummySender(), 1024L), 200L);

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

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/4/" + fullKey + "?requestId=" + reqId))
                .PUT(HttpRequest.BodyPublishers.ofString("materialized"))
                .build();
        assertEquals(200, http.send(putReq, HttpResponse.BodyHandlers.ofString()).statusCode());

        server.processSequencerMessage(4,
                new Message.FileCommitMessage(bucket, key, reqId, getDummySender(), 1024L), 300L);

        assertEquals(0, server.repairPendingCount(),
                "No repair should be enqueued when the blob was already materialized at commit time");
    }

    @Test
    void testGetOnMissingBlobReturns404WithoutEnqueueingRepair() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();
        port = server.port();

        server.processSequencerMessage(5,
                new Message.FileCommitMessage("rb", "ghost", "req-ghost", getDummySender(), 1024L), 400L);

        int repairCountAfterCommit = server.repairPendingCount();

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/store/5/rb/ghost"))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, resp.statusCode());
        assertEquals(repairCountAfterCommit, server.repairPendingCount(),
                "GET should not enqueue additional repair; only the commit path triggers repair");
    }

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

    @Test
    void testNewerCommitMissOverridesOlderForSameKey() throws Exception {
        server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
        server.start();

        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "dup-key", "req-dup1", getDummySender(), 1024L), 100L);
        server.processSequencerMessage(3,
                new Message.FileCommitMessage("bucket", "dup-key", "req-dup2", getDummySender(), 1024L), 200L);

        assertEquals(1, server.repairPendingCount(),
                "Multiple commits for the same key should compact to 1 pending repair");
    }

    @Test
    void testActualBlobRepair() throws Exception {
        byte[] originalData = "Testing actual blob repair with erasure coding across nodes.".getBytes(StandardCharsets.UTF_8);
        int k = 2;
        int n = 3;
        ErasureCoder coder = new ErasureCoder();
        InputStream[] shards;
        try (var bais = new java.io.ByteArrayInputStream(originalData)) {
            shards = coder.shard(bais, originalData.length, k, n);
        }
        byte[] shard0 = shards[0].readAllBytes();
        byte[] shard1 = shards[1].readAllBytes();
        byte[] shard2 = shards[2].readAllBytes();

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
            server = new StorageNodeServerV2(0, "127.0.0.1", db, tempDir, metadataClient, pubSubClient, repairQueue);
            server.start();
            port = server.port();

            var es = new ErasureSetConfiguration.ErasureSet();
            es.setNumber(1);
            es.setM(k);
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

            Thread.sleep(500);

            String bucket = "repair-bucket";
            String key = "repair-key-actual";
            String fullKey = bucket + "/" + key;
            String reqId = "req-repair-actual";

            ErasureRouting routing = new ErasureRouting(psConfig, esConfig);
            int partition = routing.getPartition(fullKey).orElseThrow();

            // Pass the originalData length as the logical size
            server.processSequencerMessage(partition, new Message.FileCommitMessage(bucket, key, reqId, getDummySender(), originalData.length), 100L);

            // Wait for repair queue to drain, then GET the recovered shard.
            await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> server.repairPendingCount() == 0);

            HttpRequest getReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/store/" + partition + "/" + fullKey))
                    .GET()
                    .build();

            AtomicReference<HttpResponse<byte[]>> resRef = new AtomicReference<>();
            await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                        HttpResponse<byte[]> r = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
                        resRef.set(r);
                        return r.statusCode() == 200;
                    });
            assertArrayEquals(shard0, resRef.get().body(), "Recovered shard should match original shard0 data");

        } finally {
            peer1.stop();
            peer2.stop();
            coder.shutdown();
        }
    }
}