package com.github.koop.queryprocessor.gateway;

import com.github.koop.queryprocessor.gateway.StorageServices.TcpStorageService;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests TcpStorageService (now HTTP-based) against a small Javalin mock server.
 */
class TcpStorageServiceTest {

    private TcpStorageService service;
    private Javalin mockServer;
    private int port;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        store.clear();

        mockServer = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;

            // PUT handler
            config.routes.put("/store/{partition}/{key}", ctx -> {
                String partition = ctx.pathParam("partition");
                String key = ctx.pathParam("key");
                byte[] body = ctx.bodyAsBytes();
                store.put(partition + "|" + key, body);
                ctx.status(200).result("OK");
            });

            // GET handler
            config.routes.get("/store/{partition}/{key}", ctx -> {
                String partition = ctx.pathParam("partition");
                String key = ctx.pathParam("key");
                byte[] data = store.get(partition + "|" + key);
                if (data != null) {
                    ctx.status(200)
                       .header("Content-Type", "application/octet-stream")
                       .result(data);
                } else {
                    ctx.status(404).result("");
                }
            });

            // DELETE handler
            config.routes.delete("/store/{partition}/{key}", ctx -> {
                String partition = ctx.pathParam("partition");
                String key = ctx.pathParam("key");
                store.remove(partition + "|" + key);
                ctx.status(200).result("OK");
            });
        });

        mockServer.start(0);
        port = mockServer.port();
        service = new TcpStorageService("localhost", port);
    }

    @AfterEach
    void tearDown() {
        if (mockServer != null) mockServer.stop();
    }

    @Test
    void testPutProtocolCompliance() throws Exception {
        String key = "my-test-key";
        String content = "Hello World";
        ByteArrayInputStream data = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        service.putObject("my-bucket", key, data, content.length());

        // Verify data was stored
        assertFalse(store.isEmpty(), "Store should contain the put data");

        // Find the entry and verify content
        byte[] stored = store.values().iterator().next();
        assertEquals(content, new String(stored, StandardCharsets.UTF_8));
    }

    @Test
    void testGetProtocolCompliance() throws Exception {
        String key = "get-key";
        int partition = Math.abs(key.hashCode() % 10);
        byte[] payload = "FoundIt".getBytes(StandardCharsets.UTF_8);
        store.put(partition + "|" + key, payload);

        InputStream result = service.getObject("bucket", key);
        assertNotNull(result);
        String content = new String(result.readAllBytes(), StandardCharsets.UTF_8);
        assertEquals("FoundIt", content);
        result.close();
    }

    @Test
    void testGetNotFound() throws Exception {
        InputStream result = service.getObject("bucket", "nonexistent-key");
        assertNull(result, "Should return null for missing key");
    }

    @Test
    void testDeleteProtocolCompliance() throws Exception {
        String key = "del-key";
        int partition = Math.abs(key.hashCode() % 10);
        store.put(partition + "|" + key, "data".getBytes());

        service.deleteObject("bucket", key);

        assertFalse(store.containsKey(partition + "|" + key), "Key should be deleted");
    }
}