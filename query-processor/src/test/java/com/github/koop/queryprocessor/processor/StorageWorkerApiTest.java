package com.github.koop.queryprocessor.processor;

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

    @BeforeAll
    void setup() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new FakeStorageNodeServer());

        List<InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();
        worker = new StorageWorker(set, set, set);
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

        // Under 4 failures, we should not successfully reconstruct the full object.
        // Depending on implementation, this may produce EOF/short read rather than throwing.
        assertNotEquals(data.length, got.length, "should not reconstruct full object under 4 failures");

        // Optional stronger check: if it *did* return full length (rare), it must not match.
        if (got.length == data.length) {
            assertFalse(Arrays.equals(data, got), "if full length returned, content must not match");
        }
    }

}
