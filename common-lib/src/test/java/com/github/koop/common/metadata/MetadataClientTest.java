package com.github.koop.common.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MetadataClientTest {

    private MemoryFetcher memoryFetcher;
    private MetadataClient metadataClient;

    // Dummy classes used to simulate configuration updates
    static class DummyConfigA {
        String value;
        DummyConfigA(String value) { this.value = value; }
    }

    static class DummyConfigB {
        int id;
        DummyConfigB(int id) { this.id = id; }
    }

    @BeforeEach
    void setUp() {
        memoryFetcher = new MemoryFetcher();
        metadataClient = new MetadataClient(memoryFetcher);
    }

    @AfterEach
    void tearDown() throws Exception {
        metadataClient.close();
    }

    @Test
    void clientCallsFetcherStart() {
        metadataClient.start();
        assertTrue(memoryFetcher.wasStarted(), "Fetcher should have been started");
    }

    @Test
    void clientCallsFetcherClose() throws Exception {
        metadataClient.close();
        assertTrue(memoryFetcher.wasClosed(), "Fetcher should have been closed");
    }

    @Test
    void testListenerReceivesInitialUpdate() {
        AtomicReference<DummyConfigA> prevRef = new AtomicReference<>();
        AtomicReference<DummyConfigA> currentRef = new AtomicReference<>();

        metadataClient.listen(DummyConfigA.class, (prev, current) -> {
            prevRef.set(prev);
            currentRef.set(current);
        });

        metadataClient.start();

        DummyConfigA initialConfig = new DummyConfigA("initial");
        memoryFetcher.update(initialConfig);

        assertNull(prevRef.get(), "Previous configuration should be null on the first update");
        assertNotNull(currentRef.get(), "Current configuration should not be null");
        assertEquals("initial", currentRef.get().value);
    }

    @Test
    void testListenerReceivesSubsequentUpdatesWithPrevState() {
        AtomicReference<DummyConfigA> prevRef = new AtomicReference<>();
        AtomicReference<DummyConfigA> currentRef = new AtomicReference<>();

        metadataClient.listen(DummyConfigA.class, (prev, current) -> {
            prevRef.set(prev);
            currentRef.set(current);
        });

        metadataClient.start();

        DummyConfigA v1 = new DummyConfigA("version-1");
        memoryFetcher.update(v1);

        DummyConfigA v2 = new DummyConfigA("version-2");
        memoryFetcher.update(v2);

        assertEquals(v1, prevRef.get(), "Previous configuration should be version-1");
        assertEquals(v2, currentRef.get(), "Current configuration should be version-2");
    }

    @Test
    void testListenersAreIsolatedByClass() {
        AtomicInteger configACalls = new AtomicInteger(0);
        AtomicInteger configBCalls = new AtomicInteger(0);

        metadataClient.listen(DummyConfigA.class, (prev, current) -> configACalls.incrementAndGet());
        metadataClient.listen(DummyConfigB.class, (prev, current) -> configBCalls.incrementAndGet());

        metadataClient.start();

        // Update only ConfigA
        memoryFetcher.update(new DummyConfigA("a-test"));

        assertEquals(1, configACalls.get(), "ConfigA listener should be called once");
        assertEquals(0, configBCalls.get(), "ConfigB listener should not be called");
    }

    @Test
    void testMultipleListenersForTheSameClass() {
        AtomicInteger listener1Calls = new AtomicInteger(0);
        AtomicInteger listener2Calls = new AtomicInteger(0);

        metadataClient.listen(DummyConfigA.class, (prev, current) -> listener1Calls.incrementAndGet());
        metadataClient.listen(DummyConfigA.class, (prev, current) -> listener2Calls.incrementAndGet());

        metadataClient.start();

        memoryFetcher.update(new DummyConfigA("trigger-both"));

        assertEquals(1, listener1Calls.get(), "First listener should be triggered");
        assertEquals(1, listener2Calls.get(), "Second listener should be triggered");
    }
}