package com.github.koop.storagenode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
            byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // frameLength EXCLUDES opcode
            // frameLength = ReqIdLen(4) + ReqIdBytes + Partition(4) + KeyLen(4) + KeyBytes + DataBytes
            long putLen = 4L + reqIdBytes.length + 4L + 4L + keyBytes.length + data.length;

            writeLong(out, putLen);
            writeInt(out, 1);               // Opcode (PUT)
            writeString(out, reqId);        // ReqID
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.write(data);                // Data
            out.flush();

            // Read PUT Response: server writes long(1) then byte(1/0)
            consumeResponseLength(in);
            int status = in.read();
            assertEquals(1, status, "PUT should return success (1)");

            // --- GET Request ---
            // frameLength EXCLUDES opcode
            // frameLength = Partition(4) + KeyLen(4) + KeyBytes
            long getLen = 4L + 4L + keyBytes.length;

            writeLong(out, getLen);
            writeInt(out, 6);               // Opcode (GET)
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.flush();

            // Read GET Response: long(responseLen) then byte(found) then data
            long responseLen = consumeResponseLength(in);
            int found = in.read();
            assertEquals(1, found, "GET should return found (1)");

            // responseLen includes 1 byte found flag + data bytes
            int dataLen = (int) (responseLen - 1);
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
            byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // frameLength EXCLUDES opcode
            long putLen = 4L + reqIdBytes.length + 4L + 4L + keyBytes.length + data.length;

            writeLong(out, putLen);
            writeInt(out, 1); // PUT
            writeString(out, reqId);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data);
            out.flush();

            consumeResponseLength(in);
            in.read(); // consume success

            // --- DELETE Request ---
            // frameLength EXCLUDES opcode
            // frameLength = Partition(4) + KeyLen(4) + KeyBytes
            long delLen = 4L + 4L + keyBytes.length;

            writeLong(out, delLen);
            writeInt(out, 2); // DELETE
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            consumeResponseLength(in);
            int deleted = in.read();
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

                    byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    byte[] data = value.getBytes(StandardCharsets.UTF_8);

                    // frameLength EXCLUDES opcode
                    long putLen = 4L + reqIdBytes.length + 4L + 4L + keyBytes.length + data.length;

                    writeLong(out, putLen);
                    writeInt(out, 1); // PUT
                    writeString(out, reqId);
                    writeInt(out, partition);
                    writeString(out, key);
                    out.write(data);
                    out.flush();

                    consumeResponseLength(in);
                    int status = in.read();
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
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

                // frameLength EXCLUDES opcode
                long getLen = 4L + 4L + keyBytes.length;

                writeLong(out, getLen);
                writeInt(out, 6); // GET
                writeInt(out, partition);
                writeString(out, key);
                out.flush();

                long respLen = consumeResponseLength(in);
                int found = in.read();
                assertEquals(1, found, "GET should return found (1)");

                int dataLen = (int) (respLen - 1);
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
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // First PUT
            String reqId1 = "ow-1";
            byte[] reqIdBytes1 = reqId1.getBytes(StandardCharsets.UTF_8);
            byte[] data1 = "FirstValue".getBytes(StandardCharsets.UTF_8);

            long putLen1 = 4L + reqIdBytes1.length + 4L + 4L + keyBytes.length + data1.length;

            writeLong(out, putLen1);
            writeInt(out, 1); // PUT
            writeString(out, reqId1);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data1);
            out.flush();

            consumeResponseLength(in);
            int status1 = in.read();
            assertEquals(1, status1, "First PUT should return success (1)");

            // Second PUT (overwrite)
            String reqId2 = "ow-2";
            byte[] reqIdBytes2 = reqId2.getBytes(StandardCharsets.UTF_8);
            byte[] data2 = "SecondValue".getBytes(StandardCharsets.UTF_8);

            long putLen2 = 4L + reqIdBytes2.length + 4L + 4L + keyBytes.length + data2.length;

            writeLong(out, putLen2);
            writeInt(out, 1); // PUT
            writeString(out, reqId2);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data2);
            out.flush();

            consumeResponseLength(in);
            int status2 = in.read();
            assertEquals(1, status2, "Second PUT should return success (1)");

            // GET
            long getLen = 4L + 4L + keyBytes.length;

            writeLong(out, getLen);
            writeInt(out, 6); // GET
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            long respLen = consumeResponseLength(in);
            int found = in.read();
            assertEquals(1, found, "GET after overwrite should return found (1)");

            int dataLen = (int) (respLen - 1);
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
                    byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    byte[] data = values[idx].getBytes(StandardCharsets.UTF_8);

                    // frameLength EXCLUDES opcode
                    long putLen = 4L + reqIdBytes.length + 4L + 4L + keyBytes.length + data.length;

                    writeLong(out, putLen);
                    writeInt(out, 1); // PUT
                    writeString(out, reqId);
                    writeInt(out, partition);
                    writeString(out, key);
                    out.write(data);
                    out.flush();

                    consumeResponseLength(in);
                    int status = in.read();
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

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            long getLen = 4L + 4L + keyBytes.length;

            writeLong(out, getLen);
            writeInt(out, 6); // GET
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            long respLen = consumeResponseLength(in);
            int found = in.read();
            assertEquals(1, found, "GET should return found (1)");

            int dataLen = (int) (respLen - 1);
            String got = new String(in.readNBytes(dataLen), StandardCharsets.UTF_8);

            boolean matches = false;
            for (String v : values) {
                if (v.equals(got)) { matches = true; break; }
            }
            assertEquals(true, matches, "GET should return one of the concurrently written values");
        }
    }

    // ---------- Helper Methods ----------

    private static void writeInt(OutputStream out, int val) throws IOException {
        out.write(ByteBuffer.allocate(4).putInt(val).array());
    }

    private static void writeLong(OutputStream out, long val) throws IOException {
        out.write(ByteBuffer.allocate(8).putLong(val).array());
    }

    private static void writeString(OutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt(out, bytes.length);
        out.write(bytes);
    }

    /** Reads and returns the 8-byte response length the server always sends first. */
    private static long consumeResponseLength(InputStream in) throws IOException {
        byte[] buf = in.readNBytes(8);
        if (buf.length != 8) throw new EOFException("Expected 8-byte response length");
        return ByteBuffer.wrap(buf).getLong();
    }
}
