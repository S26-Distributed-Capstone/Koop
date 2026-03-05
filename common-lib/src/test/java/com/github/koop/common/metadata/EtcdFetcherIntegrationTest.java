package com.github.koop.common.metadata;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class EtcdFetcherIntegrationTest {

    // Spin up a real Etcd instance for testing
    @Container
    public static GenericContainer<?> etcd = new GenericContainer<>("quay.io/coreos/etcd:v3.5.12")
            .withExposedPorts(2379)
            .withCommand(
                    "/usr/local/bin/etcd",
                    "--advertise-client-urls", "http://0.0.0.0:2379",
                    "--listen-client-urls", "http://0.0.0.0:2379",
                    "--initial-advertise-peer-urls", "http://0.0.0.0:2380",
                    "--listen-peer-urls", "http://0.0.0.0:2380",
                    "--initial-cluster", "default=http://0.0.0.0:2380")
            .waitingFor(Wait.forLogMessage(".*ready to serve client requests.*\\n", 1));

    private Client rawEtcdClient;
    private EtcdFetcher etcdFetcher;
    private String endpoint;
    private String testKey;

    // Dummy class for JSON deserialization testing
    static class DummyConfig {
        public String status;
        public int version;

        // Needed for Jackson
        public DummyConfig() {
        }

        public DummyConfig(String status, int version) {
            this.status = status;
            this.version = version;
        }
    }

    @BeforeEach
    void setUp() {
        endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        rawEtcdClient = Client.builder().endpoints(endpoint).build();

        // Generate a random key for each test run to guarantee isolation
        testKey = "/config/test_dummy_" + java.util.UUID.randomUUID().toString();

        Map<Class<?>, String> keyMap = Map.of(DummyConfig.class, testKey);
        etcdFetcher = new EtcdFetcher(endpoint, keyMap);
    }

    @AfterEach
    void tearDown() {
        if (etcdFetcher != null)
            etcdFetcher.close();
        if (rawEtcdClient != null)
            rawEtcdClient.close();
    }

    @Test
    void testInitialFetchRetrievesExistingData() throws Exception {
        // 1. Seed Etcd with initial data
        String initialJson = "{\"status\": \"active\", \"version\": 1}";
        putToEtcd(testKey, initialJson);

        AtomicReference<Object> receivedObj = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // 2. Start the fetcher
        etcdFetcher.start(obj -> {
            receivedObj.set(obj);
            latch.countDown();
        });

        // 3. Verify the fetcher immediately pulls the seeded data and parses it
        assertTrue(latch.await(3, TimeUnit.SECONDS), "Fetcher did not trigger initial load in time");
        assertNotNull(receivedObj.get());
        assertTrue(receivedObj.get() instanceof DummyConfig);

        DummyConfig config = (DummyConfig) receivedObj.get();
        assertEquals("active", config.status);
        assertEquals(1, config.version);
    }

    @Test
    void testWatcherPushesUpdatesOnPut() throws Exception {
        AtomicReference<Object> receivedObj = new AtomicReference<>();
        CountDownLatch updateLatch = new CountDownLatch(1);

        // 1. Start fetcher (no initial data)
        etcdFetcher.start(obj -> {
            receivedObj.set(obj);
            updateLatch.countDown();
        });

        // 2. Trigger an update in Etcd
        String updatedJson = "{\"status\": \"updated\", \"version\": 2}";
        putToEtcd(testKey, updatedJson);

        // 3. Verify the watcher caught it
        assertTrue(updateLatch.await(5, TimeUnit.SECONDS), "Watcher did not detect PUT event in time");

        DummyConfig config = (DummyConfig) receivedObj.get();
        assertEquals("updated", config.status);
        assertEquals(2, config.version);
    }

    @Test
    void testWatcherPushesNullOnDelete() throws Exception {
        // 1. Seed Etcd
        putToEtcd(testKey, "{\"status\": \"temp\", \"version\": 1}");

        AtomicReference<Object> receivedObj = new AtomicReference<>();
        CountDownLatch deleteLatch = new CountDownLatch(2); // 1 for initial load, 1 for delete

        // 2. Start fetcher
        etcdFetcher.start(obj -> {
            receivedObj.set(obj);
            deleteLatch.countDown();
        });

        // Wait for initial fetch to finish
        Thread.sleep(500);

        // 3. Delete the key from Etcd
        rawEtcdClient.getKVClient().delete(ByteSequence.from(testKey, StandardCharsets.UTF_8)).get();

        // 4. Verify the watcher caught the DELETE and passed null
        assertTrue(deleteLatch.await(5, TimeUnit.SECONDS), "Watcher did not detect DELETE event in time");
        assertNull(receivedObj.get(), "Fetcher should pass null when a key is deleted");
    }

    // Helper to write to Etcd synchronously
    private void putToEtcd(String key, String value) throws Exception {
        rawEtcdClient.getKVClient().put(
                ByteSequence.from(key, StandardCharsets.UTF_8),
                ByteSequence.from(value, StandardCharsets.UTF_8)).get();
    }
}