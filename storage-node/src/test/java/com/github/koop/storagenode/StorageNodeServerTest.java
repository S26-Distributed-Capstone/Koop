package com.github.koop.storagenode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageNodeServerTest {

    private StorageNodeServer server;
    private HttpClient http;
    private int port;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        server = new StorageNodeServer(0, tempDir);  // port 0 → OS picks free port
        var t = Thread.ofVirtual().start(server::start);
        t.join();  // Wait for server to start and set the port
        port = server.port();
        http = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    private URI storeUri(int partition, String key) {
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + key);
    }

    private URI storeUriWithReq(int partition, String key, String reqId) {
        return URI.create("http://localhost:" + port + "/store/" + partition + "/" + key + "?requestId=" + reqId);
    }

    @Test
    void testPutAndGet() throws Exception {
        int partition = 5;
        String key = "my-key";
        byte[] data = "Hello Server".getBytes(StandardCharsets.UTF_8);

        // --- PUT ---
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "req-101"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .header("Content-Type", "application/octet-stream")
                .build();

        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode(), "PUT should succeed");

        // --- GET ---
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
                .GET()
                .build();

        HttpResponse<byte[]> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, getResp.statusCode(), "GET should succeed");
        assertEquals("Hello Server", new String(getResp.body(), StandardCharsets.UTF_8));
    }

    @Test
    void testDelete() throws Exception {
        int partition = 2;
        String key = "del-key";
        byte[] data = "ToBeDeleted".getBytes(StandardCharsets.UTF_8);

        // Setup: store first
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "del-req"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .header("Content-Type", "application/octet-stream")
                .build();
        HttpResponse<String> putResp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, putResp.statusCode(), "Setup PUT should succeed");

        // --- DELETE ---
        HttpRequest delReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
                .DELETE()
                .build();

        HttpResponse<String> delResp = http.send(delReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, delResp.statusCode(), "DELETE should succeed");
    }

    @Test
    void testMultipleClientsDifferentKeys() throws Exception {
        int clientCount = 5;
        String[] keys = new String[clientCount];
        Thread[] threads = new Thread[clientCount];

        for (int i = 0; i < clientCount; i++) {
            keys[i] = "key-" + i;
            String value = "value-" + i;
            final int idx = i;

            threads[i] = new Thread(() -> {
                try {
                    HttpRequest putReq = HttpRequest.newBuilder()
                            .uri(storeUriWithReq(idx, keys[idx], "mc-" + idx))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(value.getBytes(StandardCharsets.UTF_8)))
                            .header("Content-Type", "application/octet-stream")
                            .build();

                    HttpResponse<String> resp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, resp.statusCode(), "PUT should succeed");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // Verify each key
        for (int i = 0; i < clientCount; i++) {
            HttpRequest getReq = HttpRequest.newBuilder()
                    .uri(storeUri(i, keys[i]))
                    .GET()
                    .build();

            HttpResponse<byte[]> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, getResp.statusCode(), "GET should find the key");
            assertEquals("value-" + i, new String(getResp.body(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void testOverwritePutSameKey() throws Exception {
        int partition = 1;
        String key = "overwrite-key";

        // First PUT
        HttpRequest put1 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "ow-1"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray("FirstValue".getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/octet-stream")
                .build();
        assertEquals(200, http.send(put1, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Second PUT (overwrite)
        HttpRequest put2 = HttpRequest.newBuilder()
                .uri(storeUriWithReq(partition, key, "ow-2"))
                .PUT(HttpRequest.BodyPublishers.ofByteArray("SecondValue".getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/octet-stream")
                .build();
        assertEquals(200, http.send(put2, HttpResponse.BodyHandlers.ofString()).statusCode());

        // GET should return the second value
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
                .GET()
                .build();

        HttpResponse<byte[]> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, getResp.statusCode());
        assertEquals("SecondValue", new String(getResp.body(), StandardCharsets.UTF_8));
    }

    @Test
    void testConcurrentPutSameKeyConsistency() throws Exception {
        int clientCount = 10;
        String key = "shared-key";
        int partition = 7;
        String[] values = new String[clientCount];
        Thread[] threads = new Thread[clientCount];

        for (int i = 0; i < clientCount; i++) {
            final int idx = i;
            values[idx] = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            threads[idx] = new Thread(() -> {
                try {
                    HttpRequest putReq = HttpRequest.newBuilder()
                            .uri(storeUriWithReq(partition, key, "req-" + idx))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(values[idx].getBytes(StandardCharsets.UTF_8)))
                            .header("Content-Type", "application/octet-stream")
                            .build();

                    HttpResponse<String> resp = http.send(putReq, HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, resp.statusCode(), "PUT should succeed");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // GET should return one of the written values
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(storeUri(partition, key))
                .GET()
                .build();

        HttpResponse<byte[]> getResp = http.send(getReq, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, getResp.statusCode());

        String got = new String(getResp.body(), StandardCharsets.UTF_8);
        boolean matches = false;
        for (String v : values) {
            if (v.equals(got)) { matches = true; break; }
        }
        assertTrue(matches, "GET should return one of the concurrently written values");
    }
}