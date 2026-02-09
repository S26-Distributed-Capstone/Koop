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

            // Total Length: Opcode(4) + ReqIdLen(4) + ReqIdBytes + Partition(4) + KeyLen(4) + KeyBytes + DataBytes
            long totalLength = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

            writeLong(out, totalLength);    // FIXED: Write frame length as Long
            writeInt(out, 1);               // Opcode (PUT)
            writeString(out, reqId);        // ReqID
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.write(data);                // Data
            out.flush();

            // Read PUT Response
            consumeResponseLength(in);      // FIXED: Consume 8-byte response length
            int status = in.read();         // Status (1 byte)
            assertEquals(1, status, "PUT should return success (1)");

            // --- GET Request ---
            // Length: Opcode(4) + Partition(4) + KeyLen(4) + KeyBytes
            long getLength = 4 + 4 + 4 + keyBytes.length;

            writeLong(out, getLength);      // FIXED: Write frame length as Long
            writeInt(out, 6);               // Opcode (GET)
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.flush();

            // Read GET Response
            consumeResponseLength(in);      // FIXED: Consume 8-byte response length
            int found = in.read();          // Status (1 byte)
            assertEquals(1, found, "GET should return found (1)");

            byte[] responseData = in.readNBytes(data.length);
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

            long putLen = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

            writeLong(out, putLen);         // FIXED: Long
            writeInt(out, 1); // PUT
            writeString(out, reqId);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data);
            out.flush();
            
            consumeResponseLength(in);      // FIXED
            in.read(); // consume success

            // --- DELETE Request ---
            long delLen = 4 + 4 + 4 + keyBytes.length;

            writeLong(out, delLen);         // FIXED: Long
            writeInt(out, 2); // DELETE
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            // Check Result
            consumeResponseLength(in);      // FIXED
            int deleted = in.read();
            assertEquals(1, deleted, "DELETE should return success (1)");
        }
    }

    @Test
    void testMultipleClientsDifferentKeys() throws Exception {
        int clientCount = 5;
        Thread[] threads = new Thread[clientCount];
        String[] keys = new String[clientCount];
        String[] values = new String[clientCount];

        for (int i = 0; i < clientCount; i++) {
            final int idx = i;
            keys[i] = "key-" + idx;
            values[i] = "value-" + idx;
            threads[i] = new Thread(() -> {
                try (Socket socket = new Socket("localhost", PORT);
                     OutputStream out = socket.getOutputStream();
                     InputStream in = socket.getInputStream()) {

                    String reqId = "req-" + idx;
                    int partition = 1;
                    byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
                    byte[] keyBytes = keys[idx].getBytes(StandardCharsets.UTF_8);
                    byte[] data = values[idx].getBytes(StandardCharsets.UTF_8);

                    long totalLength = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

                    writeLong(out, totalLength); // FIXED: Long
                    writeInt(out, 1); // PUT
                    writeString(out, reqId);
                    writeInt(out, partition);
                    writeString(out, keys[idx]);
                    out.write(data);
                    out.flush();

                    consumeResponseLength(in);   // FIXED
                    int status = in.read();
                    assertEquals(1, status, "PUT should return success (1)");

                    // GET
                    long getLength = 4 + 4 + 4 + keyBytes.length;
                    writeLong(out, getLength);   // FIXED: Long
                    writeInt(out, 6); // GET
                    writeInt(out, partition);
                    writeString(out, keys[idx]);
                    out.flush();

                    consumeResponseLength(in);   // FIXED
                    int found = in.read();
                    assertEquals(1, found, "GET should return found (1)");
                    byte[] responseData = in.readNBytes(data.length);
                    assertEquals(values[idx], new String(responseData, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
    }

    @Test
    void testPutOverwriteSameKey() throws IOException {
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            String reqId1 = "req-1";
            String reqId2 = "req-2";
            int partition = 3;
            String key = "overwrite-key";
            byte[] data1 = "FirstValue".getBytes(StandardCharsets.UTF_8);
            byte[] data2 = "SecondValue".getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // First PUT
            byte[] reqIdBytes1 = reqId1.getBytes(StandardCharsets.UTF_8);
            long putLen1 = 4 + 4 + reqIdBytes1.length + 4 + 4 + keyBytes.length + data1.length;

            writeLong(out, putLen1); // FIXED: Long
            writeInt(out, 1); // PUT
            writeString(out, reqId1);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data1);
            out.flush();
            
            consumeResponseLength(in); // FIXED
            int status1 = in.read();
            assertEquals(1, status1, "First PUT should return success (1)");

            // Second PUT (overwrite)
            byte[] reqIdBytes2 = reqId2.getBytes(StandardCharsets.UTF_8);
            long putLen2 = 4 + 4 + reqIdBytes2.length + 4 + 4 + keyBytes.length + data2.length;

            writeLong(out, putLen2); // FIXED: Long
            writeInt(out, 1); // PUT
            writeString(out, reqId2);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data2);
            out.flush();
            
            consumeResponseLength(in); // FIXED
            int status2 = in.read();
            assertEquals(1, status2, "Second PUT should return success (1)");

            // GET
            long getLen = 4 + 4 + 4 + keyBytes.length;
            writeLong(out, getLen); // FIXED: Long
            writeInt(out, 6); // GET
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            consumeResponseLength(in); // FIXED
            int found = in.read();
            assertEquals(1, found, "GET after overwrite should return found (1)");
            byte[] responseData = in.readNBytes(data2.length);
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

                    long totalLength = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

                    writeLong(out, totalLength); // FIXED: Long
                    writeInt(out, 1); // PUT
                    writeString(out, reqId);
                    writeInt(out, partition);
                    writeString(out, key);
                    out.write(data);
                    out.flush();

                    consumeResponseLength(in); // FIXED
                    int status = in.read();
                    assertEquals(1, status, "PUT should return success (1)");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // GET verification
        try (Socket socket = new Socket("localhost", PORT);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            long getLen = 4 + 4 + 4 + keyBytes.length;
            
            writeLong(out, getLen); // FIXED: Long
            writeInt(out, 6); // GET
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            consumeResponseLength(in); // FIXED
            int found = in.read();
            assertEquals(1, found, "GET after concurrent PUTs should return found (1)");

            // Read the value (16 bytes)
            byte[] responseData = in.readNBytes(16);
            String candidate = new String(responseData, StandardCharsets.UTF_8);
            
            boolean foundMatch = false;
            for (String expected : values) {
                if (candidate.equals(expected)) {
                    foundMatch = true;
                    break;
                }
            }
            assertEquals(true, foundMatch, "GET result should be one of the concurrently written values");
        }
    }

    private void writeInt(OutputStream out, int v) throws IOException {
        out.write(ByteBuffer.allocate(4).putInt(v).array());
    }
    
    // New Helper for Protocol Compliance
    private void writeLong(OutputStream out, long v) throws IOException {
        out.write(ByteBuffer.allocate(8).putLong(v).array());
    }

    private void writeString(OutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        writeInt(out, b.length);
        out.write(b);
    }
    
    // New Helper to consume the 8-byte response length
    private void consumeResponseLength(InputStream in) throws IOException {
        byte[] b = in.readNBytes(8);
        if (b.length < 8) {
             throw new EOFException("Unexpected EOF while reading response length");
        }
    }
}