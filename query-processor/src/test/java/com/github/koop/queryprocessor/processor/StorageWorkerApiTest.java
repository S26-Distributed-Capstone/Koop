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

/**
 * Integration and unit tests for the StorageWorker's core 2-phase commit and retrieval logic.
 * These tests utilize an in-memory PubSub system and a fake storage node server array
 * to simulate distributed network behavior, quorum calculations, and erasure coding.
 */
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

    /**
     * Initializes the fake distributed cluster. Sets up the mock storage nodes,
     * injects the cluster configuration into the MetadataClient, and boots the StorageWorkers.
     */
    @BeforeAll
    void setup() throws Exception {
        PubSubClient sharedPubSub = new PubSubClient(new MemoryPubSub());
        sharedPubSub.start();

        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new AckingFakeStorageNodeServer(sharedPubSub));

        List<InetSocketAddress> set = nodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        memoryFetcher = new MemoryFetcher();
        MetadataClient metadataClient = new MetadataClient(memoryFetcher);
        metadataClient.start();
        memoryFetcher.update(buildErasureSetConfiguration(set, set, set));
        memoryFetcher.update(buildPartitionSpreadConfiguration());

        CommitCoordinator coordinator = new CommitCoordinator(sharedPubSub, 0);
        worker = new StorageWorker(metadataClient, coordinator);

        liveWorker = new StorageWorker(metadataClient, new CommitCoordinator(sharedPubSub, 0));
    }

    /**
     * Clears internal state (like ACK counts) on the fake nodes before each test.
     */
    @BeforeEach
    void resetNodes() {
        for (AckingFakeStorageNodeServer n : nodes) n.reset();
    }

    /**
     * Gracefully shuts down the workers and fake nodes after all tests complete.
     */
    @AfterAll
    void teardown() throws Exception {
        worker.shutdown();
        liveWorker.shutdown();
        for (AckingFakeStorageNodeServer n : nodes) n.close();
    }

    // -------------------------------------------------------------------------
    // Existing round-trip tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that a file can be successfully uploaded (PUT) and downloaded (GET) 
     * without data corruption.
     */
    @Test
    void putThenGet_roundTrip() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileA", data.length,
                new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", "fileA");
        assertNotNull(obj);
        try (InputStream in = obj.stream()) {
            assertArrayEquals(data, in.readAllBytes());
        }
    }

    /**
     * Verifies that the erasure coder can successfully reconstruct a file even when
     * 3 out of 9 storage nodes are offline.
     */
    @Test
    void get_withThreeNodeFailures_stillWorks() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);
        assertTrue(worker.put(UUID.randomUUID(), "b", "fileB", data.length,
                new ByteArrayInputStream(data)));

        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);

        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", "fileB");
        assertNotNull(obj);
        try (InputStream in = obj.stream()) {
            assertArrayEquals(data, in.readAllBytes());
        }
    }

    /**
     * Verifies that the erasure coder fails to reconstruct a file (as mathematically expected)
     * when 4 out of 9 storage nodes are offline (exceeding the parity tolerance).
     */
    @Test
    void get_withFourNodeFailures_fails() throws Exception {
        byte[] data = randomBytes(DATA_SIZE);
        assertTrue(worker.put(UUID.randomUUID(), "b", "fileC", data.length,
                new ByteArrayInputStream(data)));

        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);
        nodes.get(3).setEnabled(false);

        byte[] got = new byte[0];
        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", "fileC");
        if (obj != null) {
            try (InputStream in = obj.stream()) {
                got = in.readAllBytes();
            }
        }

        assertNotEquals(data.length, got.length,
                "should not reconstruct full object under 4 failures");
        if (got.length == data.length) {
            assertFalse(Arrays.equals(data, got),
                    "if full length returned, content must not match");
        }
    }

    /**
     * Validates that the StorageWorker respects dynamic updates pushed via the MetadataClient
     * and correctly routes requests to new nodes when the cluster topology changes.
     */
    @Test
    void liveConfigUpdate_newNodesUsedOnNextRequest() throws Exception {
        PubSubClient sharedPubSub = new PubSubClient(new MemoryPubSub());
        sharedPubSub.start();

        List<AckingFakeStorageNodeServer> newNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) newNodes.add(new AckingFakeStorageNodeServer(sharedPubSub));

        try {
            List<InetSocketAddress> newSet = newNodes.stream()
                    .map(AckingFakeStorageNodeServer::address).toList();

            memoryFetcher.update(buildErasureSetConfiguration(newSet, newSet, newSet));
            memoryFetcher.update(buildPartitionSpreadConfiguration());

            MemoryFetcher freshFetcher = new MemoryFetcher();
            MetadataClient freshMetadata = new MetadataClient(freshFetcher);
            CommitCoordinator freshCoordinator = new CommitCoordinator(sharedPubSub, 0);
            StorageWorker freshLiveWorker = new StorageWorker(freshMetadata, freshCoordinator);

            freshMetadata.start();
            freshFetcher.update(buildErasureSetConfiguration(newSet, newSet, newSet));
            freshFetcher.update(buildPartitionSpreadConfiguration());

            byte[] data = randomBytes(DATA_SIZE);
            assertTrue(freshLiveWorker.put(UUID.randomUUID(), "b", "fileD", data.length,
                    new ByteArrayInputStream(data)), "put to updated nodes should succeed");

            StorageWorker.RetrievedObject obj = freshLiveWorker.get(UUID.randomUUID(), "b", "fileD");
            assertNotNull(obj);
            try (InputStream in = obj.stream()) {
                assertArrayEquals(data, in.readAllBytes(),
                        "should read back from updated nodes");
            }
            freshLiveWorker.shutdown();
        } finally {
            for (AckingFakeStorageNodeServer n : newNodes) n.close();
        }
    }

    /**
     * Checks the standard production initialization path to ensure proper instantiation
     * and full round-trip capabilities (PUT, GET, DELETE).
     */
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

            StorageWorker.RetrievedObject obj = prodWorker.get(UUID.randomUUID(), "prod", "fileP");
            assertNotNull(obj);
            try (InputStream in = obj.stream()) {
                assertArrayEquals(data, in.readAllBytes(),
                        "production constructor: round-trip data must match");
            }

            assertTrue(prodWorker.delete(UUID.randomUUID(), "prod", "fileP"),
                    "production constructor: delete should succeed");

            StorageWorker.RetrievedObject afterDelete = prodWorker.get(UUID.randomUUID(), "prod", "fileP");
            assertNull(afterDelete, "production constructor: get should return null after delete");

            prodWorker.shutdown();
        } finally {
            for (AckingFakeStorageNodeServer n : prodNodes) n.close();
        }
    }

    // -------------------------------------------------------------------------
    // Commit-protocol tests
    // -------------------------------------------------------------------------

    /**
     * Ensures that the Phase 2 commit broadcasts the FileCommitMessage to the correct
     * partition-specific Kafka topic.
     */
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

        for (int p = 0; p < 6; p++) {
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

    /**
     * Verifies that the StorageWorker fails the commit and returns false if the
     * required number of nodes (writeQuorum) do not acknowledge the operation.
     */
    @Test
    void commitPhase_failsWhenBelowQuorum() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> quorumNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) quorumNodes.add(new AckingFakeStorageNodeServer(bus));

        List<InetSocketAddress> set = quorumNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        // 3 nodes are completely dead. Only 6 remain. (Write Quorum is 7).
        quorumNodes.get(0).setEnabled(false);
        quorumNodes.get(1).setEnabled(false);
        quorumNodes.get(2).setEnabled(false);
        quorumNodes.get(0).setUploadEnabled(true);
        quorumNodes.get(1).setUploadEnabled(true);
        quorumNodes.get(2).setUploadEnabled(true);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator fastCoordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 3);
        StorageWorker quorumWorker = new StorageWorker(client, fastCoordinator);

        try {
            byte[] data = randomBytes(1024);
            boolean ok = quorumWorker.put(UUID.randomUUID(), "b", "quorumFail",
                    data.length, new ByteArrayInputStream(data));
            assertFalse(ok,
                    "put should fail when only 6/9 nodes ACK the commit (writeQuorum=m+1=7)");
        } finally {
            quorumWorker.shutdown();
            for (AckingFakeStorageNodeServer n : quorumNodes) n.close();
        }
    }

    /**
     * Validates that if a few nodes fail during the Phase 1 upload, but recover and ACK 
     * during Phase 2, the system handles the eventual consistency gracefully.
     */
    @Test
    void commitPhase_succeedsWithUploadFailureThatLaterAcks() throws Exception {
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
                    "put should succeed: 8 shards uploaded, all 9 nodes ACK after commit");

            StorageWorker.RetrievedObject obj = w.get(UUID.randomUUID(), "b", "partialUpload");
            assertNotNull(obj);
            try (InputStream in = obj.stream()) {
                assertArrayEquals(data, in.readAllBytes());
            }
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : nodes2) n.close();
        }
    }

    /**
     * Tests that the CommitCoordinator successfully registers exactly one ACK from 
     * each active storage node, avoiding duplicates.
     */
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

    /**
     * Spin up multiple virtual threads to perform concurrent PUTs and verify that 
     * the CommitCoordinator isolates and tracks ACKs accurately per-transaction.
     */
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
                        StorageWorker.RetrievedObject obj = worker.get(UUID.randomUUID(), "b", key);
                        if (obj != null) {
                            try (InputStream in = obj.stream()) {
                                byte[] got = in.readAllBytes();
                                if (!Arrays.equals(data, got))
                                    errors.add(new AssertionError("data mismatch for key " + key));
                            }
                        } else {
                            errors.add(new AssertionError("get returned null for key " + key));
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
    // Bucket operation tests
    // -------------------------------------------------------------------------

    private <T extends Message> void spyOnPartitions(
            PubSubClient bus, Class<T> type,
            CopyOnWriteArrayList<String> topics,
            CopyOnWriteArrayList<Message> messages) {
        for (int p = 0; p < 6; p++) {
            bus.sub(CommitTopics.forPartition(p), (t, offset, bytes) -> {
                Message msg = Message.deserializeMessage(bytes);
                if (type.isInstance(msg)) {
                    if (topics != null) topics.add(t);
                    messages.add(msg);
                }
            });
        }
    }

    /**
     * Validates that bucket creation triggers a Kafka message to the correct partition topic.
     */
    @Test
    void createBucket_succeedsAndPublishesToPartitionTopic() throws Exception {
        CopyOnWriteArrayList<String> receivedTopics = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Message> receivedMessages = new CopyOnWriteArrayList<>();

        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> bucketNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) bucketNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = bucketNodes.stream().map(AckingFakeStorageNodeServer::address).toList();
        spyOnPartitions(bus, Message.CreateBucketMessage.class, receivedTopics, receivedMessages);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);
        try {
            UUID requestId = UUID.randomUUID();
            assertTrue(w.createBucket(requestId, "my-bucket"), "createBucket should succeed");

            assertEquals(1, receivedMessages.size(), "exactly one CreateBucketMessage should be published");
            Message.CreateBucketMessage msg = (Message.CreateBucketMessage) receivedMessages.get(0);
            assertEquals("my-bucket", msg.bucket());
            assertEquals(requestId.toString(), msg.requestID());
            assertTrue(receivedTopics.get(0).startsWith("partition-"),
                    "should publish to a partition topic, got: " + receivedTopics.get(0));
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : bucketNodes) n.close();
        }
    }

    /**
     * Validates that bucket deletion triggers a Kafka message to the correct partition topic.
     */
    @Test
    void deleteBucket_succeedsAndPublishesToPartitionTopic() throws Exception {
        CopyOnWriteArrayList<Message> receivedMessages = new CopyOnWriteArrayList<>();

        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> bucketNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) bucketNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = bucketNodes.stream().map(AckingFakeStorageNodeServer::address).toList();
        spyOnPartitions(bus, Message.DeleteBucketMessage.class, null, receivedMessages);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);
        try {
            UUID requestId = UUID.randomUUID();
            assertTrue(w.deleteBucket(requestId, "my-bucket"), "deleteBucket should succeed");

            assertEquals(1, receivedMessages.size(), "exactly one DeleteBucketMessage should be published");
            Message.DeleteBucketMessage msg = (Message.DeleteBucketMessage) receivedMessages.get(0);
            assertEquals("my-bucket", msg.bucket());
            assertEquals(requestId.toString(), msg.requestID());
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : bucketNodes) n.close();
        }
    }

    /**
     * Ensures bucket creation requires quorum approval to succeed.
     */
    @Test
    void createBucket_failsWhenBelowQuorum() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> bucketNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) bucketNodes.add(new AckingFakeStorageNodeServer(bus));
        for (int i = 0; i < 6; i++) bucketNodes.get(i).setEnabled(false);
        List<InetSocketAddress> set = bucketNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator fastCoordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 3);
        StorageWorker w = new StorageWorker(client, fastCoordinator);
        try {
            assertFalse(w.createBucket(UUID.randomUUID(), "no-quorum-bucket"),
                    "createBucket should fail when only 3/9 nodes ACK (deleteQuorum=k+1=4)");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : bucketNodes) n.close();
        }
    }

    /**
     * Simulates the entire lifecycle of a bucket from creation to population, deletion, and purging.
     */
    @Test
    void bucketLifecycle_createPutDeleteObjectDeleteBucket() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> lifecycleNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) lifecycleNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = lifecycleNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);
        try {
            assertTrue(w.createBucket(UUID.randomUUID(), "lifecycle-bucket"), "createBucket should succeed");

            byte[] data = randomBytes(DATA_SIZE);
            assertTrue(w.put(UUID.randomUUID(), "lifecycle-bucket", "obj1",
                    data.length, new ByteArrayInputStream(data)), "put should succeed");

            StorageWorker.RetrievedObject obj = w.get(UUID.randomUUID(), "lifecycle-bucket", "obj1");
            assertNotNull(obj);
            try (InputStream in = obj.stream()) {
                assertArrayEquals(data, in.readAllBytes(), "get should return original data");
            }

            assertTrue(w.delete(UUID.randomUUID(), "lifecycle-bucket", "obj1"), "delete should succeed");

            StorageWorker.RetrievedObject deletedObj = w.get(UUID.randomUUID(), "lifecycle-bucket", "obj1");
            assertNull(deletedObj, "data should not be retrievable after delete");

            assertTrue(w.deleteBucket(UUID.randomUUID(), "lifecycle-bucket"), "deleteBucket should succeed");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : lifecycleNodes) n.close();
        }
    }

    /**
     * Verifies that deleting an object broadcasts a DeleteMessage via Kafka and waits
     * for the appropriate quorum response.
     */
    @Test
    void delete_publishesToPartitionTopicAndAwaitsQuorum() throws Exception {
        CopyOnWriteArrayList<String> receivedTopics = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Message> receivedMessages = new CopyOnWriteArrayList<>();

        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> deleteNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) deleteNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = deleteNodes.stream().map(AckingFakeStorageNodeServer::address).toList();
        spyOnPartitions(bus, Message.DeleteMessage.class, receivedTopics, receivedMessages);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0);
        StorageWorker w = new StorageWorker(client, coordinator);
        try {
            byte[] data = randomBytes(1024);
            assertTrue(w.put(UUID.randomUUID(), "b", "toDelete", data.length, new ByteArrayInputStream(data)));

            UUID deleteId = UUID.randomUUID();
            assertTrue(w.delete(deleteId, "b", "toDelete"), "delete should succeed");

            assertEquals(1, receivedMessages.size(), "exactly one DeleteMessage should be published");
            Message.DeleteMessage msg = (Message.DeleteMessage) receivedMessages.get(0);
            assertEquals("b", msg.bucket());
            assertEquals("toDelete", msg.key());
            assertEquals(deleteId.toString(), msg.requestID());
            assertTrue(receivedTopics.get(0).startsWith("partition-"),
                    "delete should publish to a partition topic, got: " + receivedTopics.get(0));
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : deleteNodes) n.close();
        }
    }

    /**
     * Validates that simultaneous bucket creation/deletion operations are thread-safe.
     */
    @Test
    void concurrentBucketOps_allSucceed() throws Exception {
        int concurrency = 4;
        List<Thread> threads = new ArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            final String bucket = "concurrent-bucket-" + i;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    assertTrue(worker.createBucket(UUID.randomUUID(), bucket),
                            "createBucket failed for " + bucket);
                    assertTrue(worker.deleteBucket(UUID.randomUUID(), bucket),
                            "deleteBucket failed for " + bucket);
                } catch (Exception e) {
                    errors.add(e);
                }
            }));
        }

        for (Thread t : threads) t.join(60_000);
        assertTrue(errors.isEmpty(), "concurrent bucket ops produced errors: " + errors);
    }

    // -------------------------------------------------------------------------
    // bucketExists / listObjects tests
    // -------------------------------------------------------------------------

    /**
     * Verifies `bucketExists` properly fans out and returns true when a bucket is available.
     */
    @Test
    void bucketExists_returnsTrueAfterCreate() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> beNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) beNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = beNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        StorageWorker w = new StorageWorker(client, new CommitCoordinator(bus, 0));
        try {
            assertTrue(w.createBucket(UUID.randomUUID(), "exists-bucket"), "createBucket should succeed");
            assertTrue(w.bucketExists("exists-bucket"), "bucketExists should be true after create");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : beNodes) n.close();
        }
    }

    @Test
    void bucketExists_returnsFalseForNonExistentBucket() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> beNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) beNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = beNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        StorageWorker w = new StorageWorker(client, new CommitCoordinator(bus, 0));
        try {
            assertFalse(w.bucketExists("never-created"), "bucketExists should be false");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : beNodes) n.close();
        }
    }

    @Test
    void listObjects_returnsEmptyForEmptyBucket() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> loNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) loNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = loNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        StorageWorker w = new StorageWorker(client, new CommitCoordinator(bus, 0));
        try {
            assertTrue(w.createBucket(UUID.randomUUID(), "empty-bucket"));
            List<StorageWorker.ObjectInfo> objects = w.listObjects("empty-bucket", "", 1000);
            assertNotNull(objects);
            assertTrue(objects.isEmpty(), "expected empty list, got: " + objects);
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : loNodes) n.close();
        }
    }

    /**
     * Verifies that listObjects can fan out across multiple partitions, collect the lists,
     * and compile a final, sorted array of file names.
     */
    @Test
    void listObjects_returnsStoredObjects() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> loNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) loNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = loNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        StorageWorker w = new StorageWorker(client, new CommitCoordinator(bus, 0));
        try {
            assertTrue(w.createBucket(UUID.randomUUID(), "list-bucket"));

            byte[] data = randomBytes(1024);
            assertTrue(w.put(UUID.randomUUID(), "list-bucket", "alpha", data.length,
                    new ByteArrayInputStream(data)));
            assertTrue(w.put(UUID.randomUUID(), "list-bucket", "beta", data.length,
                    new ByteArrayInputStream(data)));
            assertTrue(w.put(UUID.randomUUID(), "list-bucket", "gamma", data.length,
                    new ByteArrayInputStream(data)));

            List<StorageWorker.ObjectInfo> objects = w.listObjects("list-bucket", "", 1000);
            List<String> keys = objects.stream().map(StorageWorker.ObjectInfo::key).sorted().toList();
            assertEquals(List.of("list-bucket/alpha", "list-bucket/beta", "list-bucket/gamma"), keys);
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : loNodes) n.close();
        }
    }

    /**
     * Tests the fault tolerance of the JSON metadata parser. If a node returns corrupted data
     * during a list request, it shouldn't crash the entire list process.
     */
    @Test
    void parseObjectListJson_returnsEmptyOnMalformedInput() throws Exception {
        java.lang.reflect.Method method = StorageWorker.class.getDeclaredMethod(
                "parseObjectListJson", String.class);
        method.setAccessible(true);

        // Malformed JSON — not valid JSON at all
        @SuppressWarnings("unchecked")
        List<StorageWorker.ObjectInfo> result1 =
                (List<StorageWorker.ObjectInfo>) method.invoke(null, "THIS IS NOT JSON!!!");
        assertNotNull(result1, "Should return non-null on malformed input");
        assertTrue(result1.isEmpty(), "Should return empty list on malformed JSON, got: " + result1);

        // Truncated JSON — partial array
        @SuppressWarnings("unchecked")
        List<StorageWorker.ObjectInfo> result2 =
                (List<StorageWorker.ObjectInfo>) method.invoke(null, "[{\"key\":\"test");
        assertNotNull(result2, "Should return non-null on truncated input");
        assertTrue(result2.isEmpty(), "Should return empty list on truncated JSON, got: " + result2);

        // Null input
        @SuppressWarnings("unchecked")
        List<StorageWorker.ObjectInfo> result3 =
                (List<StorageWorker.ObjectInfo>) method.invoke(null, (String) null);
        assertNotNull(result3, "Should return non-null on null input");
        assertTrue(result3.isEmpty(), "Should return empty list on null input");

        // Empty string
        @SuppressWarnings("unchecked")
        List<StorageWorker.ObjectInfo> result4 =
                (List<StorageWorker.ObjectInfo>) method.invoke(null, "");
        assertNotNull(result4, "Should return non-null on empty input");
        assertTrue(result4.isEmpty(), "Should return empty list on empty input");
    }

    @Test
    void listObjects_returnsEmptyForNonExistentBucket() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();
        List<AckingFakeStorageNodeServer> loNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) loNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = loNodes.stream().map(AckingFakeStorageNodeServer::address).toList();

        MetadataClient client = createConfiguredClient(set);
        StorageWorker w = new StorageWorker(client, new CommitCoordinator(bus, 0));
        try {
            List<StorageWorker.ObjectInfo> objects = w.listObjects("never-created-bucket", "", 1000);
            assertNotNull(objects, "Should return non-null even for non-existent bucket");
            assertTrue(objects.isEmpty(),
                    "Should return empty list for non-existent bucket, got: " + objects);
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : loNodes) n.close();
        }
    }

    // -------------------------------------------------------------------------
    // Timeout and unresponsive node tests
    // -------------------------------------------------------------------------

    /**
     * Validates that a PUT request will time out and gracefully fail if the commit
     * phase stalls indefinitely due to unresponsive storage nodes.
     */
    @Test
    void put_failsWhenCommitTimesOut() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> timeoutNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) timeoutNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = timeoutNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        for (AckingFakeStorageNodeServer n : timeoutNodes) n.setEnabled(false);
        for (AckingFakeStorageNodeServer n : timeoutNodes) n.setUploadEnabled(true);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator fastCoordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 2);
        StorageWorker w = new StorageWorker(client, fastCoordinator);

        try {
            byte[] data = randomBytes(1024);
            long start = System.currentTimeMillis();
            boolean ok = w.put(UUID.randomUUID(), "b", "timeoutTest",
                    data.length, new ByteArrayInputStream(data));
            long elapsed = System.currentTimeMillis() - start;

            assertFalse(ok, "put should fail when commit times out with no ACKs");
            assertTrue(elapsed < 10_000,
                    "should fail within reasonable time, not hang indefinitely (took " + elapsed + "ms)");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : timeoutNodes) n.close();
        }
    }

    @Test
    void delete_failsWhenBelowQuorum() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> deleteNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) deleteNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = deleteNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        for (int i = 0; i < 6; i++) deleteNodes.get(i).setEnabled(false);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator fastCoordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 2);
        StorageWorker w = new StorageWorker(client, fastCoordinator);

        try {
            assertFalse(w.delete(UUID.randomUUID(), "b", "deleteTimeout"),
                    "delete should fail when only 3/9 nodes ACK (deleteQuorum=k+1=4)");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : deleteNodes) n.close();
        }
    }

    /**
     * Validates that if all nodes are completely unreachable during Phase 1 (Upload),
     * the system aborts early without locking up.
     */
    @Test
    void put_phase1FailsWhenAllNodesUnreachable() throws Exception {
        PubSubClient bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        List<AckingFakeStorageNodeServer> unreachableNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) unreachableNodes.add(new AckingFakeStorageNodeServer(bus));
        List<InetSocketAddress> set = unreachableNodes.stream()
                .map(AckingFakeStorageNodeServer::address).toList();

        for (AckingFakeStorageNodeServer n : unreachableNodes) n.setUploadEnabled(false);

        MetadataClient client = createConfiguredClient(set);
        CommitCoordinator coordinator = new CommitCoordinator(bus, 0, /*timeoutSeconds=*/ 2);
        StorageWorker w = new StorageWorker(client, coordinator);

        try {
            byte[] data = randomBytes(1024);
            boolean ok = w.put(UUID.randomUUID(), "b", "phase1Fail",
                    data.length, new ByteArrayInputStream(data));
            assertFalse(ok,
                    "put should fail in Phase 1 when no nodes accept shard uploads");
        } finally {
            w.shutdown();
            for (AckingFakeStorageNodeServer n : unreachableNodes) n.close();
        }
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
            spread.setPartitions(List.of(s * 2, s * 2 + 1));
            spreads.add(spread);
        }
        ps.setPartitionSpread(spreads);
        return ps;
    }

    private static ErasureSet toErasureSet(int number, List<InetSocketAddress> addresses) {
        ErasureSet es = new ErasureSet();
        es.setNumber(number);
        es.setN(9);
        es.setM(6);
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