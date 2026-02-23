package com.github.koop.storagenode;

import com.github.koop.common.messages.InputStreamMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.Opcode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageNodeServerTest {

    private static final int PORT = 9092;
    private StorageNodeServer server;
    private ExecutorService serverExecutor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        server = new StorageNodeServer(PORT, tempDir);
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> server.start());
        Thread.sleep(100);
    }

    @AfterEach
    void tearDown() {
        serverExecutor.shutdownNow();
        server.stop();
    }

    @Test
    void testPutAndGet() throws IOException {
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // --- PUT Request ---
            String reqId = "req-101";
            int partition = 5;
            String key = "my-key";
            byte[] data = "Hello Server".getBytes(StandardCharsets.UTF_8);

            MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
            putMsg.writeString(reqId);
            putMsg.writeInt(partition);
            putMsg.writeString(key);
            putMsg.writeLargePayload(data.length, new ByteArrayInputStream(data));
            putMsg.writeToOutputStream(out);
            out.flush();

            // Read PUT Response
            InputStreamMessageReader putResp = new InputStreamMessageReader(in);
            assertEquals(Opcode.SN_PUT.getCode(), putResp.getOpcode(), "Response should have correct opcode");
            int status = putResp.readByte();
            assertEquals(1, status, "PUT should return success (1)");

            // --- GET Request ---
            MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
            getMsg.writeInt(partition);
            getMsg.writeString(key);
            getMsg.writeToOutputStream(out);
            out.flush();

            // Read GET Response
            InputStreamMessageReader getResp = new InputStreamMessageReader(in);
            assertEquals(Opcode.SN_GET.getCode(), getResp.getOpcode(), "Response should have correct opcode");
            int found = getResp.readByte();
            assertEquals(1, found, "GET should return found (1)");

            // Read remaining bytes for the payload directly from the InputStream
            int dataLen = (int) getResp.getRemainingLength();
            byte[] responseData = in.readNBytes(dataLen);

            assertEquals("Hello Server", new String(responseData, StandardCharsets.UTF_8));
        }
    }

    @Test
    void testDelete() throws IOException {
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Setup: Store a file first
            String reqId = "del-req";
            int partition = 2;
            String key = "del-key";
            byte[] data = "ToBeDeleted".getBytes(StandardCharsets.UTF_8);

            MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
            putMsg.writeString(reqId);
            putMsg.writeInt(partition);
            putMsg.writeString(key);
            putMsg.writeLargePayload(data.length, new ByteArrayInputStream(data));
            putMsg.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader putResp = new InputStreamMessageReader(in);
            int status = putResp.readByte(); // consume success
            assertEquals(1, status, "Setup PUT should succeed");

            // --- DELETE Request ---
            MessageBuilder delMsg = new MessageBuilder(Opcode.SN_DELETE);
            delMsg.writeInt(partition);
            delMsg.writeString(key);
            delMsg.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader delResp = new InputStreamMessageReader(in);
            assertEquals(Opcode.SN_DELETE.getCode(), delResp.getOpcode(), "Response should have correct opcode");
            int deleted = delResp.readByte();
            assertEquals(1, deleted, "DELETE should return success (1)");
        }
    }

    @Test
    void testMultipleClientsDifferentKeys() throws Exception {
        int clientCount = 5;
        Thread[] threads = new Thread[clientCount];
        String[] keys = new String[clientCount];

        for (int i = 0; i < clientCount; i++) {
            keys[i] = "key-" + i;
            String value = "value-" + i;
            final int idx = i;

            threads[i] = new Thread(() -> {
                try (Socket socket = new Socket("localhost", PORT);
                     OutputStream out = socket.getOutputStream();
                     InputStream in = socket.getInputStream()) {

                    String reqId = "mc-" + idx;
                    int partition = idx;
                    String key = keys[idx];
                    byte[] data = value.getBytes(StandardCharsets.UTF_8);

                    MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
                    putMsg.writeString(reqId);
                    putMsg.writeInt(partition);
                    putMsg.writeString(key);
                    putMsg.writeLargePayload(data.length, new ByteArrayInputStream(data));
                    putMsg.writeToOutputStream(out);
                    out.flush();

                    InputStreamMessageReader putResp = new InputStreamMessageReader(in);
                    int status = putResp.readByte();
                    assertEquals(1, status, "PUT should return success (1)");

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // Verify each key
        for (int i = 0; i < clientCount; i++) {
            try (Socket socket = new Socket("localhost", PORT);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                int partition = i;
                String key = keys[i];

                MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
                getMsg.writeInt(partition);
                getMsg.writeString(key);
                getMsg.writeToOutputStream(out);
                out.flush();

                InputStreamMessageReader getResp = new InputStreamMessageReader(in);
                int found = getResp.readByte();
                assertEquals(1, found, "GET should return found (1)");

                int dataLen = (int) getResp.getRemainingLength();
                byte[] responseData = in.readNBytes(dataLen);

                assertEquals("value-" + i, new String(responseData, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testOverwritePutSameKey() throws IOException {
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            int partition = 1;
            String key = "overwrite-key";

            // First PUT
            String reqId1 = "ow-1";
            byte[] data1 = "FirstValue".getBytes(StandardCharsets.UTF_8);

            MessageBuilder putMsg1 = new MessageBuilder(Opcode.SN_PUT);
            putMsg1.writeString(reqId1);
            putMsg1.writeInt(partition);
            putMsg1.writeString(key);
            putMsg1.writeLargePayload(data1.length, new ByteArrayInputStream(data1));
            putMsg1.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader putResp1 = new InputStreamMessageReader(in);
            int status1 = putResp1.readByte();
            assertEquals(1, status1, "First PUT should return success (1)");

            // Second PUT (overwrite)
            String reqId2 = "ow-2";
            byte[] data2 = "SecondValue".getBytes(StandardCharsets.UTF_8);

            MessageBuilder putMsg2 = new MessageBuilder(Opcode.SN_PUT);
            putMsg2.writeString(reqId2);
            putMsg2.writeInt(partition);
            putMsg2.writeString(key);
            putMsg2.writeLargePayload(data2.length, new ByteArrayInputStream(data2));
            putMsg2.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader putResp2 = new InputStreamMessageReader(in);
            int status2 = putResp2.readByte();
            assertEquals(1, status2, "Second PUT should return success (1)");

            // GET
            MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
            getMsg.writeInt(partition);
            getMsg.writeString(key);
            getMsg.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader getResp = new InputStreamMessageReader(in);
            int found = getResp.readByte();
            assertEquals(1, found, "GET after overwrite should return found (1)");

            int dataLen = (int) getResp.getRemainingLength();
            byte[] responseData = in.readNBytes(dataLen);

            assertEquals("SecondValue", new String(responseData, StandardCharsets.UTF_8));
        }
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
                try (Socket socket = new Socket("localhost", PORT);
                     OutputStream out = socket.getOutputStream();
                     InputStream in = socket.getInputStream()) {

                    String reqId = "req-" + idx;
                    byte[] data = values[idx].getBytes(StandardCharsets.UTF_8);

                    MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
                    putMsg.writeString(reqId);
                    putMsg.writeInt(partition);
                    putMsg.writeString(key);
                    putMsg.writeLargePayload(data.length, new ByteArrayInputStream(data));
                    putMsg.writeToOutputStream(out);
                    out.flush();

                    InputStreamMessageReader putResp = new InputStreamMessageReader(in);
                    int status = putResp.readByte();
                    assertEquals(1, status, "PUT should return success (1)");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // GET verification: should be one of the values
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
            getMsg.writeInt(partition);
            getMsg.writeString(key);
            getMsg.writeToOutputStream(out);
            out.flush();

            InputStreamMessageReader getResp = new InputStreamMessageReader(in);
            int found = getResp.readByte();
            assertEquals(1, found, "GET should return found (1)");

            int dataLen = (int) getResp.getRemainingLength();
            String got = new String(in.readNBytes(dataLen), StandardCharsets.UTF_8);

            boolean matches = false;
            for (String v : values) {
                if (v.equals(got)) { matches = true; break; }
            }
            assertTrue(matches, "GET should return one of the concurrently written values");
        }
    }
}