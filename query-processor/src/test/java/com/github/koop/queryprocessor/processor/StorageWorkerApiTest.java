package com.github.koop.queryprocessor.processor;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageWorkerApiTest {

    private List<FakeStorageNodeServer> nodes;
    private StorageWorker worker;

    @BeforeAll
    void setup() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new FakeStorageNodeServer());

        List<InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();

        // Use same set for all 3 erasure sets for tests
        worker = new StorageWorker(set, set, set);
    }

    @BeforeEach
    void resetNodesUp() {
        for (FakeStorageNodeServer n : nodes) n.setEnabled(true);
    }

    @AfterAll
    void teardown() throws Exception {
        for (FakeStorageNodeServer n : nodes) n.close();
    }

    @Test
    void putThenGet_roundTrip() throws Exception {
        byte[] data = new byte[500_000_000]; // 500MB
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
        byte[] data = new byte[500_000_000]; // 500MB
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
        byte[] data = new byte[500_000_000]; // 500MB
        new SecureRandom().nextBytes(data);

        boolean ok = worker.put(UUID.randomUUID(), "b", "fileC", data.length, new ByteArrayInputStream(data));
        assertTrue(ok, "put should succeed");

        // Simulate 4 node failures (NOT tolerated)
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);
        nodes.get(3).setEnabled(false);

        try (InputStream in = worker.get(UUID.randomUUID(), "b", "fileC")) {
            // Depending on your get() implementation, this may return EOF early or throw.
            // The safest assertion: it should NOT return the full correct data.
            byte[] got = in.readAllBytes();
            assertNotEquals(data.length, got.length, "should not successfully reconstruct under 4 failures");
        }
    }
}
