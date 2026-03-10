package com.github.koop.queryprocessor.processor;

import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.ReplicaSetConfiguration;
import com.github.koop.common.metadata.ReplicaSetConfiguration.Machine;
import com.github.koop.common.metadata.ReplicaSetConfiguration.ReplicaSet;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageWorkerApiTest {

    // Keep it big enough to span multiple stripes (stripe = K * 1MB = 6MB)
    private static final int DATA_SIZE = 15 * 1024 * 1024;

    private List<FakeStorageNodeServer> nodes;
    private StorageWorker worker;
    private MemoryFetcher memoryFetcher;

    @BeforeAll
    void setup() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new FakeStorageNodeServer());

        List<InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();

        // Three-arg constructor handles all the metadata wiring internally,
        // so setup stays as simple as the original test.
        worker = new StorageWorker(set, set, set);

        // Also keep a handle on a MemoryFetcher-backed worker for the live
        // update test, constructed with the correct listener-before-update ordering.
        memoryFetcher = new MemoryFetcher();
        MetadataClient metadataClient = new MetadataClient(memoryFetcher);
        // Worker must be constructed before update() so its listener is registered.
        StorageWorker liveWorker = new StorageWorker(metadataClient);
        memoryFetcher.update(buildReplicaSetConfiguration(set, set, set));
        // liveWorker is only used in liveConfigUpdate_newNodesUsedOnNextRequest;
        // store it so we can swap it in that test.
        this.liveWorker = liveWorker;
    }

    // Separate worker used only for the live-update test.
    private StorageWorker liveWorker;

    @BeforeEach
    void resetNodes() {
        for (FakeStorageNodeServer n : nodes) n.setEnabled(true);
    }

    @AfterAll
    void teardown() throws Exception {
        for (FakeStorageNodeServer n : nodes) n.close();
    }

    @Test
    void putThenGet_roundTrip() throws Exception {
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileA", data.length, new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileA")) {
            byte[] got = in.readAllBytes();
            assertArrayEquals(data, got);
        }
    }

    @Test
    void get_withThreeNodeFailures_stillWorks() throws Exception {
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileB", data.length, new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        // Simulate 3 node failures (M=3 tolerated)
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);

        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileB")) {
            byte[] got = in.readAllBytes();
            assertArrayEquals(data, got);
        }
    }

    @Test
    void get_withFourNodeFailures_fails() throws Exception {
        byte[] data = new byte[DATA_SIZE];
        new SecureRandom().nextBytes(data);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileC", data.length, new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        // Simulate 4 node failures (NOT tolerated)
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);
        nodes.get(3).setEnabled(false);

        byte[] got;
        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileC")) {
            got = in.readAllBytes();
        }

        // Under 4 failures, we should not successfully reconstruct the full object.
        // Depending on implementation, this may produce EOF/short read rather than throwing.
        assertNotEquals(data.length, got.length, "should not reconstruct full object under 4 failures");

        // Optional stronger check: if it *did* return full length (rare), it must not match.
        if (got.length == data.length) {
            assertFalse(Arrays.equals(data, got), "if full length returned, content must not match");
        }
    }

    @Test
    void liveConfigUpdate_newNodesUsedOnNextRequest() throws Exception {
        List<FakeStorageNodeServer> newNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) newNodes.add(new FakeStorageNodeServer());

        try {
            List<InetSocketAddress> newSet = newNodes.stream().map(FakeStorageNodeServer::address).toList();

            // Push updated config — liveWorker's listener fires synchronously.
            memoryFetcher.update(buildReplicaSetConfiguration(newSet, newSet, newSet));

            byte[] data = new byte[DATA_SIZE];
            new SecureRandom().nextBytes(data);

            boolean ok = liveWorker.put(UUID.randomUUID(), "b", "fileD", data.length, new ByteArrayInputStream(data));
            assertTrue(ok, "put to updated nodes should succeed");

            try (InputStream in = liveWorker.get(UUID.randomUUID(), "b", "fileD")) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(data, got, "should read back from updated nodes");
            }
        } finally {
            for (FakeStorageNodeServer n : newNodes) n.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ReplicaSetConfiguration buildReplicaSetConfiguration(
            List<InetSocketAddress> set1,
            List<InetSocketAddress> set2,
            List<InetSocketAddress> set3) {
        ReplicaSetConfiguration config = new ReplicaSetConfiguration();
        config.setReplicaSets(List.of(
                toReplicaSet(1, set1),
                toReplicaSet(2, set2),
                toReplicaSet(3, set3)));
        return config;
    }

    private static ReplicaSet toReplicaSet(int number, List<InetSocketAddress> addresses) {
        ReplicaSet rs = new ReplicaSet();
        rs.setNumber(number);
        rs.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return rs;
    }
}