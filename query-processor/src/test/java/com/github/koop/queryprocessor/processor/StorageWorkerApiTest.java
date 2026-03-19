package com.github.koop.queryprocessor.processor;

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

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageWorkerApiTest {

    // Keep it big enough to span multiple stripes (stripe = K * 1MB = 6MB)
    private static final int DATA_SIZE = 15 * 1024 * 1024;

    private List<FakeStorageNodeServer> nodes;
    private StorageWorker worker;
    private StorageWorker liveWorker;
    private MemoryFetcher memoryFetcher;

    @BeforeAll
    void setup() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new FakeStorageNodeServer());

        List<InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();

        // Three-arg constructor handles all the metadata wiring internally,
        // so setup stays as simple as the original test.
        worker = new StorageWorker(set, set, set);

        // Build a separate MetadataClient-backed worker for the live update test.
        // Worker must be constructed before update() so its listeners are registered.
        memoryFetcher = new MemoryFetcher();
        MetadataClient metadataClient = new MetadataClient(memoryFetcher);
        liveWorker = new StorageWorker(metadataClient);
        // Push both configs — order doesn't matter, both listeners are already registered.
        memoryFetcher.update(buildErasureSetConfiguration(set, set, set));
        memoryFetcher.update(buildPartitionSpreadConfiguration());
    }

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

        assertNotEquals(data.length, got.length, "should not reconstruct full object under 4 failures");
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

            // Push updated replica set and partition spread — listeners fire synchronously.
            memoryFetcher.update(buildErasureSetConfiguration(newSet, newSet, newSet));
            memoryFetcher.update(buildPartitionSpreadConfiguration());

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


    @Test
    void productionConstructor_putGetDelete_roundTrip() throws Exception {
        // Spin up a dedicated set of 9 nodes so this test is fully isolated
        // from the shared nodes used by the other tests.
        List<FakeStorageNodeServer> prodNodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) prodNodes.add(new FakeStorageNodeServer());

        try {
            List<InetSocketAddress> set = prodNodes.stream().map(FakeStorageNodeServer::address).toList();

            // Use the production MetadataClient constructor directly — no testing
            // shortcut. Worker is constructed first so listeners are registered,
            // then both configs are pushed via the MemoryFetcher.
            MemoryFetcher fetcher = new MemoryFetcher();
            MetadataClient client = new MetadataClient(fetcher);
            StorageWorker prodWorker = new StorageWorker(client);
            fetcher.update(buildErasureSetConfiguration(set, set, set));
            fetcher.update(buildPartitionSpreadConfiguration());

            byte[] data = new byte[DATA_SIZE];
            new SecureRandom().nextBytes(data);

            // PUT
            boolean ok = prodWorker.put(UUID.randomUUID(), "prod", "fileP", data.length, new ByteArrayInputStream(data));
            assertTrue(ok, "production constructor: put should succeed");

            // GET
            try (InputStream in = prodWorker.get(UUID.randomUUID(), "prod", "fileP")) {
                byte[] got = in.readAllBytes();
                assertArrayEquals(data, got, "production constructor: round-trip data must match");
            }

            // DELETE then confirm GET returns nothing / short data
            boolean deleted = prodWorker.delete(UUID.randomUUID(), "prod", "fileP");
            assertTrue(deleted, "production constructor: delete should succeed");

            try (InputStream in = prodWorker.get(UUID.randomUUID(), "prod", "fileP")) {
                byte[] got = in.readAllBytes();
                assertNotEquals(data.length, got.length,
                        "production constructor: data should not be retrievable after delete");
            }
        } finally {
            for (FakeStorageNodeServer n : prodNodes) n.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    /**
     * Builds a PartitionSpreadConfiguration matching the one baked into the
     * testing constructor: 99 partitions spread evenly, 33 per set.
     */
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
        es.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return es;
    }
}