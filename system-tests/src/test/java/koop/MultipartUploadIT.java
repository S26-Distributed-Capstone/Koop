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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for multipart upload using real {@link StorageNodeServerV2} instances.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultipartUploadIT {

    private static final int TOTAL_NODES = 9;

    private final List<StorageNodeServerV2> servers = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private final List<Database> databases = new ArrayList<>();
    private final List<Integer> nodePorts = new ArrayList<>();

    private StorageWorker worker;
    private MultipartUploadManager manager;
    private MetadataClient metadataClient;
    private PubSubClient pubSubClient;

    @BeforeAll
    void startCluster() throws Exception {
        List<InetSocketAddress> addrs = new ArrayList<>();

        MemoryFetcher fetcher = new MemoryFetcher();
        metadataClient = new MetadataClient(fetcher);
        pubSubClient = new PubSubClient(new MemoryPubSub());
        pubSubClient.start();
        CommitCoordinator commitCoordinator = new CommitCoordinator(pubSubClient, 0);

        metadataClient.start();
        worker = new StorageWorker(metadataClient, commitCoordinator);
        manager = new MultipartUploadManager(worker, new MemoryCacheClient());

        for (int i = 0; i < TOTAL_NODES; i++) {
            int port = freePort();
            nodePorts.add(port);
            Path dir = Files.createTempDirectory("mpu-it-node-" + i + "-");
            dataDirs.add(dir);

            Database db = new Database(new RocksDbStorageStrategy(dir.resolve("db").toString()));
            databases.add(db);

            StorageNodeServerV2 server = new StorageNodeServerV2(
                    port,
                    "127.0.0.1",
                    db,
                    dir.resolve("data"),
                    metadataClient,
                    pubSubClient);
            servers.add(server);
            server.start();
            addrs.add(new InetSocketAddress("127.0.0.1", port));
        }

        fetcher.update(buildErasureSetConfiguration(addrs, addrs, addrs));
        fetcher.update(buildPartitionSpreadConfiguration());
    }

    @BeforeEach
    void resetServers() {
        // All nodes are real and always up; nothing to reset between tests.
    }

    @AfterAll
    void stopCluster() throws Exception {
        if (pubSubClient != null) {
            pubSubClient.close();
        }
        if (metadataClient != null) {
            metadataClient.close();
        }

        for (StorageNodeServerV2 server : servers) {
            server.stop();
        }
        for (Database database : databases) {
            try {
                database.close();
            } catch (Exception ignored) {
            }
        }
        for (Path dir : dataDirs) {
            deleteRecursive(dir);
        }
        if (worker != null) {
            worker.shutdown();
        }
    }

    @Test
    void fullLifecycle_initUploadComplete_thenGet() throws Exception {
        byte[] p1 = "hello ".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "world".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-1");
        MultipartUploadResult upload1 = manager.uploadPart(
            "bucket", "mpu-obj-1", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        MultipartUploadResult upload2 = manager.uploadPart(
            "bucket", "mpu-obj-1", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        assertEquals(MultipartUploadResult.Status.SUCCESS, upload1.status());
        assertEquals(MultipartUploadResult.Status.SUCCESS, upload2.status());

        MultipartUploadResult complete = manager.completeMultipartUpload(
                "bucket",
                "mpu-obj-1",
                uploadId,
                List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));
        assertEquals(MultipartUploadResult.Status.SUCCESS, complete.status());

        String part1Key = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-1", uploadId, 1);
        String part2Key = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-1", uploadId, 2);
        try (InputStream in1 = worker.get(UUID.randomUUID(), "bucket", part1Key);
             InputStream in2 = worker.get(UUID.randomUUID(), "bucket", part2Key)) {
            assertArrayEquals(p1, in1.readAllBytes());
            assertArrayEquals(p2, in2.readAllBytes());
        }
    }

    @Test
    void fullLifecycle_initUploadAbort_partsGone() throws Exception {
        byte[] payload = "temporary-part".getBytes(StandardCharsets.UTF_8);

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-2");
        MultipartUploadResult upload = manager.uploadPart(
            "bucket", "mpu-obj-2", uploadId, 1, new ByteArrayInputStream(payload), payload.length);
        assertEquals(MultipartUploadResult.Status.SUCCESS, upload.status());

        String partStorageKey = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-2", uploadId, 1);
        MultipartUploadResult abort = manager.abortMultipartUpload("bucket", "mpu-obj-2", uploadId);
        assertEquals(MultipartUploadResult.Status.SUCCESS, abort.status());

        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "bucket", partStorageKey)) {
            got = in.readAllBytes();
        } catch (Exception ignored) {
            got = new byte[0];
        }

        assertNotEquals(payload.length, got.length,
                "aborted part should no longer be reconstructable");
    }

    @Test
    void complete_partialNodeFailure_stillSucceeds() throws Exception {
        byte[] p1 = new byte[2 * 1024 * 1024];
        byte[] p2 = new byte[2 * 1024 * 1024];
        Arrays.fill(p1, (byte) 'A');
        Arrays.fill(p2, (byte) 'B');

        String uploadId = manager.initiateMultipartUpload("bucket", "mpu-obj-3");
        MultipartUploadResult upload1 = manager.uploadPart(
            "bucket", "mpu-obj-3", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        MultipartUploadResult upload2 = manager.uploadPart(
            "bucket", "mpu-obj-3", uploadId, 2, new ByteArrayInputStream(p2), p2.length);
        assertEquals(MultipartUploadResult.Status.SUCCESS, upload1.status());
        assertEquals(MultipartUploadResult.Status.SUCCESS, upload2.status());

        servers.get(0).stop();
        servers.get(1).stop();
        servers.get(2).stop();

        try {
            MultipartUploadResult complete = manager.completeMultipartUpload(
                    "bucket",
                    "mpu-obj-3",
                    uploadId,
                    List.of(new StorageService.CompletedPart(1), new StorageService.CompletedPart(2)));
            assertEquals(MultipartUploadResult.Status.SUCCESS, complete.status());

            String part1Key = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-3", uploadId, 1);
            String part2Key = MultipartUploadSession.partStorageKey("bucket", "mpu-obj-3", uploadId, 2);

            try (InputStream p1In = worker.get(UUID.randomUUID(), "bucket", part1Key);
                 InputStream p2In = worker.get(UUID.randomUUID(), "bucket", part2Key)) {
                byte[] p1Read = p1In.readAllBytes();
                byte[] p2Read = p2In.readAllBytes();
                assertTrue(p1Read.length == p1.length, "part 1 length should match expected");
                assertTrue(p2Read.length == p2.length, "part 2 length should match expected");
                assertTrue(p1Read[0] == 'A', "part 1 first byte should be 'A'");
                assertTrue(p2Read[0] == 'B', "part 2 first byte should be 'B'");
            }
        } finally {
            for (int i = 0; i < 3; i++) {
                int port = nodePorts.get(i);
                Path dir = dataDirs.get(i);
                Database db = databases.get(i);
                StorageNodeServerV2 replacement = new StorageNodeServerV2(
                        port,
                        "127.0.0.1",
                        db,
                        dir.resolve("data"),
                        metadataClient,
                        pubSubClient);
                servers.set(i, replacement);
                replacement.start();
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    private static ErasureSetConfiguration buildErasureSetConfiguration(
            List<InetSocketAddress> set1,
            List<InetSocketAddress> set2,
            List<InetSocketAddress> set3) {
        ErasureSetConfiguration config = new ErasureSetConfiguration();
        config.setErasureSets(List.of(
                toErasureSet(1, set1),
                toErasureSet(2, set2),
                toErasureSet(3, set3)));
        return config;
    }

    private static PartitionSpreadConfiguration buildPartitionSpreadConfiguration() {
        PartitionSpreadConfiguration ps = new PartitionSpreadConfiguration();
        List<PartitionSpread> spreads = new ArrayList<>();

        for (int s = 0; s < 3; s++) {
            PartitionSpread spread = new PartitionSpread();
            spread.setErasureSet(s + 1);
            List<Integer> partitions = new ArrayList<>();
            for (int p = s * 33; p < (s + 1) * 33; p++) {
                partitions.add(p);
            }
            spread.setPartitions(partitions);
            spreads.add(spread);
        }
        ps.setPartitionSpread(spreads);
        return ps;
    }

    private static ErasureSet toErasureSet(int number, List<InetSocketAddress> addresses) {
        ErasureSet es = new ErasureSet();
        es.setNumber(number);
        es.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return es;
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