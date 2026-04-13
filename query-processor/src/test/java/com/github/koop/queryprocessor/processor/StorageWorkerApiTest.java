package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.Message;
import com.github.koop.common.messages.Message.FileCommitMessage;
import com.github.koop.common.pubsub.CommitTopics;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration.PartitionSpread;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageWorkerApiTest {

    // Big enough to span multiple stripes (stripe = K * 1MB = 6MB).
    private static final int DATA_SIZE = 15 * 1024 * 1024;

    private List<AckingFakeStorageNodeServer> nodes;
    private StorageWorker worker;
    private StorageWorker liveWorker;
    private MemoryFetcher memoryFetcher;

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    @BeforeAll
    void setup() throws Exception {
        // Build a shared PubSubClient backed by MemoryPubSub. Both the worker's
        // CommitCoordinator and the fake SNs share this bus so that commit messages
        // published by the coordinator are immediately visible to the fake SNs,
        // which then POST their ACKs back to the coordinator's HTTP server.
        PubSubClient sharedPubSub = new PubSubClient(new MemoryPubSub());
        sharedPubSub.start();

        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new AckingFakeStorageNodeServer(sharedPubSub));

        List<InetSocketAddress> set = nodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        // Build a MetadataClient-backed worker
        memoryFetcher = new MemoryFetcher();
        MetadataClient metadataClient = new MetadataClient(memoryFetcher);
        metadataClient.start();
        memoryFetcher.update(buildErasureSetConfiguration(set, set, set));
        memoryFetcher.update(buildPartitionSpreadConfiguration());

        CommitCoordinator coordinator = new CommitCoordinator(sharedPubSub, 0);
        worker = new StorageWorker(metadataClient, coordinator);

        liveWorker = new StorageWorker(metadataClient, new CommitCoordinator(sharedPubSub, 0));
    }

    @BeforeEach
    void resetNodes() {
        for (AckingFakeStorageNodeServer n : nodes) n.reset();
    }

    @AfterAll
    void teardown() throws Exception {
        worker.shutdown();
        liveWorker.shutdown();
        for (AckingFakeStorageNodeServer n : nodes) n.close();
    }

    // -------------------------------------------------------------------------
    // Existing round-trip tests (updated to use AckingFakeStorageNodeServer)
    // -------------------------------------------------------------------------

    @Test
    void putThenGet_roundTrip() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileA", data.length,
                new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileA")) {
            assertArrayEquals(data, in.readAllBytes());
        }
    }

    @Test
    void get_withThreeNodeFailures_stillWorks() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);
        assertTrue(worker.put(UUID.randomUUID(), "b", "fileB", data.length,
                new ByteArrayInputStream(data)));

        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);

        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileB")) {
            assertArrayEquals(data, in.readAllBytes());
        }
    }

    @Test
    void get_withFourNodeFailures_fails() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);
        assertTrue(worker.put(UUID.randomUUID(), "b", "fileC", data.length,
                new ByteArrayInputStream(data)));

        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);
        nodes.get(3).setEnabled(false);

        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileC")) {
            got = in.readAllBytes();
        }

        assertNotEquals(data.length, got.length,
                "should not reconstruct full object under 4 failures");
        if (got.length == data.length) {
            assertFalse(Arrays.equals(data, got),
                    "if full length returned, content must not match");
        }
    }

    @Test
    void liveConfigUpdate_newNodesUsedOnNextRequest() throws Exception {
        PubSubClient sharedPubSub = new PubSubClient(new MemoryPubSub());
        sharedPubSub.start();

        List<AckingFakeStorageNodeServer> newNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) newNodes.add(new AckingFakeStorageNodeServer(sharedPubSub));

        try {
            List<InetSocketAddress> newSet = newNodes.stream()
                    .map(AckingFakeStorageNodeServer::address).toList();
            var metadataClient = new MetadataClient(memoryFetcher);
            metadataClient.start();
            memoryFetcher.update(buildErasureSetConfiguration(newSet, newSet, newSet));
            memoryFetcher.update(buildPartitionSpreadConfiguration());

            // Wire up a fresh coordinator on the same bus so the new SNs' ACKs
            // reach it.  We need to rebuild liveWorker's coordinator here because
            // the old one is subscribed to the old sharedPubSub.
            CommitCoordinator freshCoordinator = new CommitCoordinator(sharedPubSub, 0);
            StorageWorker freshLiveWorker = new StorageWorker(
                    metadataClient, freshCoordinator);
            memoryFetcher.update(buildErasureSetConfiguration(newSet, newSet, newSet));
            memoryFetcher.update(buildPartitionSpreadConfiguration());

            byte[] data = randomBytes(DATA_SIZE);
            assertTrue(freshLiveWorker.put(UUID.randomUUID(), "b", "fileD", data.length,
                    new ByteArrayInputStream(data)), "put to updated nodes should succeed");

            try (InputStream in = freshLiveWorker.get(UUID.randomUUID(), "b", "fileD")) {
                assertArrayEquals(data, in.readAllBytes(),
                        "should read back from updated nodes");
            }
            freshLiveWorker.shutdown();
        } finally {
            for (AckingFakeStorageNodeServer n : newNodes) n.close();
        }
    }

    @Test
    void productionConstructor_putGetDelete_roundTrip() throws Exception {
        PubSubClient sharedPubSub = new PubSubClient(new MemoryPubSub());
        sharedPubSub.start();

        List<AckingFakeStorageNodeServer> prodNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) prodNodes.add(new AckingFakeStorageNodeServer(sharedPubSub));

        try {
            List<InetSocketAddress> set = prodNodes.stream()
                    .map(AckingFakeStorageNodeServer::address).toList();

            MetadataClient client = createConfiguredClient(set);
            CommitCoordinator coordinator = new CommitCoordinator(sharedPubSub, 0);
            StorageWorker prodWorker = new StorageWorker(client, coordinator);

            byte[] data = randomBytes(DATA_SIZE);

            assertTrue(prodWorker.put(UUID.randomUUID(), "prod", "fileP", data.length,
                            new ByteArrayInputStream(data)),
                    "production constructor: put should succeed");

            try (InputStream in = prodWorker.get(UUID.randomUUID(), "prod", "fileP")) {
                assertArrayEquals(data, in.readAllBytes(),
                        "production constructor: round-trip data must match");
            }

            assertTrue(prodWorker.delete(UUID.randomUUID(), "prod", "fileP"),
                    "production constructor: delete should succeed");

            try (InputStream in = prodWorker.get(UUID.randomUUID(), "prod", "fileP")) {
                assertNotEquals(data.length, in.readAllBytes().length,
                        "production constructor: data should not be retrievable after delete");
            }

            prodWorker.shutdown();
        } finally {
            for (AckingFakeStorageNodeServer n : prodNodes) n.close();
        }
    }

    // -------------------------------------------------------------------------
    // New: commit-protocol tests
    // -------------------------------------------------------------------------

    @Test
    void commitPublishedToPartitionTopic() throws Exception {
        PubSubClient spy = new PubSubClient(new MemoryPubSub());
        spy.start();

        List<AckingFakeStorageNodeServer> spyNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) spyNodes.add(new AckingFakeStorageNodeServer(spy));

        List<InetSocketAddress> set = spyNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        CopyOnWriteArrayList<String> receivedTopics = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Message> receivedMessages = new CopyOnWriteArrayList<>();

        for (int p = 0; p < 99; p++) {
            String topic = CommitTopics.forPartition(p);
            spy.sub(topic, (t, offset, bytes) -> {
                receivedTopics.add(t);
                receivedMessages.add(Message.deserializeMessage(bytes));
            });
        }

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(spy, 0);
        StorageWorker spyWorker = new StorageWorker(client, coordinator);

        try {
            byte[] data = randomBytes(1024);
            UUID requestId = UUID.randomUUID();
            boolean ok = spyWorker.put(requestId, "bucket", "topicTest",
                    data.length, new ByteArrayInputStream(data));
            assertTrue(ok, "put should succeed");

            assertEquals(1, receivedMessages.size(),
                    "expected exactly one commit message");

            assertInstanceOf(FileCommitMessage.class, receivedMessages.get(0));
            FileCommitMessage msg = (FileCommitMessage) receivedMessages.get(0);
            assertEquals("bucket", msg.bucket());
            assertEquals("topicTest", msg.key());
            assertEquals(requestId.toString(), msg.requestID());

            String publishedTopic = receivedTopics.get(0);
            assertTrue(publishedTopic.startsWith("partition-"),
                    "topic should be partition-based, got: " + publishedTopic);

            int partitionNum = Integer.parseInt(publishedTopic.substring("partition-".length()));
            assertEquals(CommitTopics.forPartition(partitionNum), publishedTopic);
        } finally {
            spyWorker.shutdown();
            for (AckingFakeStorageNodeServer n : spyNodes) n.close();
        }
    }

    @Test
    void commitPhase_failsWhenBelowQuorum() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> quorumNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) quorumNodes.add(new AckingFakeStorageNodeServer(bus));

        List<InetSocketAddress> set = quorumNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        quorumNodes.get(0).setEnabled(false);
        quorumNodes.get(1).setEnabled(false);
        quorumNodes.get(2).setEnabled(false);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator fastCoordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 3);
        StorageWorker quorumWorker = new StorageWorker(client, fastCoordinator);

        try {
            byte[] data = randomBytes(1024);
            boolean ok = quorumWorker.put(UUID.randomUUID(), "b", "quorumFail",
                    data.length, new ByteArrayInputStream(data));
            assertFalse(ok,
                    "put should fail when only 6/9 nodes ACK the commit (quorum=7)");
        } finally {
            quorumWorker.shutdown();
            for (AckingFakeStorageNodeServer n : quorumNodes) n.close();
        }
    }

    @Test
    void commitPhase_succeedsWithTwoUploadFailuresThatLaterAck() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> nodes2 = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes2.add(new AckingFakeStorageNodeServer(bus));

        List<InetSocketAddress> set = nodes2.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        nodes2.get(7).setUploadEnabled(false);
        nodes2.get(8).setUploadEnabled(false);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);

        try {
            byte[] data = randomBytes(DATA_SIZE);
            boolean ok = w.put(UUID.randomUUID(), "b", "partialUpload",
                    data.length, new ByteArrayInputStream(data));
            assertTrue(ok,
                    "put should succeed: 7 shards uploaded, all 9 nodes ACK after commit");

            try (InputStream in = w.get(UUID.randomUUID(), "b", "partialUpload")) {
                assertArrayEquals(data, in.readAllBytes());
            }
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : nodes2) n.close();
        }
    }

    @Test
    void commitPhase_eachEnabledNodeAcksExactlyOnce() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> ackNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) ackNodes.add(new AckingFakeStorageNodeServer(bus));

        List<InetSocketAddress> set = ackNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);

        try {
            UUID id = UUID.randomUUID();
            byte[] data = randomBytes(1024);
            assertTrue(w.put(id, "b", "ackCount", data.length,
                    new ByteArrayInputStream(data)));

            int totalAcks = ackNodes.stream().mapToInt(AckingFakeStorageNodeServer::acksSent).sum();
            assertEquals(9, totalAcks,
                    "all 9 enabled nodes should each have sent exactly 1 ACK");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : ackNodes) n.close();
        }
    }

    @Test
    void concurrentPuts_allSucceed() throws Exception {
        int concurrency = 5;
        List<Thread> threads = new ArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            final String key = "concurrent-" + i;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    byte[] data = randomBytes(DATA_SIZE);
                    boolean ok = worker.put(UUID.randomUUID(), "b", key,
                            data.length, new ByteArrayInputStream(data));
                    if (!ok) errors.add(new AssertionError("put failed for key " + key));
                    else {
                        try (InputStream in = worker.get(UUID.randomUUID(), "b", key)) {
                            byte[] got = in.readAllBytes();
                            if (!Arrays.equals(data, got))
                                errors.add(new AssertionError("data mismatch for key " + key));
                        }
                    }
                } catch (Exception e) {
                    errors.add(e);
                }
            }));
        }

        for (Thread t : threads) t.join(60_000);
        assertTrue(errors.isEmpty(), "concurrent puts produced errors: " + errors);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        new SecureRandom().nextBytes(b);
        return b;
    }

    private static MetadataClient createConfiguredClient(List<InetSocketAddress> set) {
        MemoryFetcher fetcher = new MemoryFetcher();
        MetadataClient client = new MetadataClient(fetcher);
        client.start();
        fetcher.update(buildErasureSetConfiguration(set, set, set));
        fetcher.update(buildPartitionSpreadConfiguration());
        return client;
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
            for (int p = s * 33; p < (s + 1) * 33; p++) partitions.add(p);
            spread.setPartitions(partitions);
            spreads.add(spread);
        }
        ps.setPartitionSpread(spreads);
        return ps;
    }

    private static ErasureSet toErasureSet(int number, List<InetSocketAddress> addresses) {
        ErasureSet es = new ErasureSet();
        es.setNumber(number);
        es.setN(9);
        es.setK(6);
        es.setReadQuorum(6);
        es.setWriteQuorum(7);
        es.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return es;
    }
}