package com.github.koop.storagenode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.lang.reflect.Field;
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

            // Calculate Lengths
            byte[] reqIdBytes = reqId.getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // Total Length calculation:
            // Opcode(4) + ReqIdLen(4) + ReqIdBytes + Partition(4) + KeyLen(4) + KeyBytes + DataBytes
            int totalLength = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

            // Send Frame
            writeInt(out, totalLength);     // Frame Length
            writeInt(out, 1);               // Opcode (PUT)
            writeString(out, reqId);        // ReqID
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.write(data);                // Data
            out.flush();

            // Read PUT Response (1 byte: 1=success)
            int status = in.read();
            assertEquals(1, status, "PUT should return success (1)");

            // --- GET Request ---
            // Calculate Length for GET: Opcode(4) + Partition(4) + KeyLen(4) + KeyBytes
            int getLength = 4 + 4 + 4 + keyBytes.length;

            writeInt(out, getLength);       // Frame Length
            writeInt(out, 6);               // Opcode (GET)
            writeInt(out, partition);       // Partition
            writeString(out, key);          // Key
            out.flush();

            // Read GET Response
            int found = in.read();
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
            byte[] data = "ToBeDeleted".getBytes();
            
            byte[] reqIdBytes = reqId.getBytes();
            byte[] keyBytes = key.getBytes();
            int putLen = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

            writeInt(out, putLen);
            writeInt(out, 1); // PUT
            writeString(out, reqId);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data);
            out.flush();
            in.read(); // consume success

            // --- DELETE Request ---
            // Length: Opcode(4) + Partition(4) + KeyLen(4) + KeyBytes
            int delLen = 4 + 4 + 4 + keyBytes.length;

            writeInt(out, delLen);
            writeInt(out, 2); // DELETE
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            // Check Result
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

                    int totalLength = 4 + 4 + reqIdBytes.length + 4 + 4 + keyBytes.length + data.length;

                    writeInt(out, totalLength);
                    writeInt(out, 1); // PUT
                    writeString(out, reqId);
                    writeInt(out, partition);
                    writeString(out, keys[idx]);
                    out.write(data);
                    out.flush();

                    int status = in.read();
                    assertEquals(1, status, "PUT should return success (1)");

                    // GET
                    int getLength = 4 + 4 + 4 + keyBytes.length;
                    writeInt(out, getLength);
                    writeInt(out, 6); // GET
                    writeInt(out, partition);
                    writeString(out, keys[idx]);
                    out.flush();

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

            // First PUT
            byte[] reqIdBytes1 = reqId1.getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            int putLen1 = 4 + 4 + reqIdBytes1.length + 4 + 4 + keyBytes.length + data1.length;

            writeInt(out, putLen1);
            writeInt(out, 1); // PUT
            writeString(out, reqId1);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data1);
            out.flush();
            int status1 = in.read();
            assertEquals(1, status1, "First PUT should return success (1)");

            // Second PUT (overwrite)
            byte[] reqIdBytes2 = reqId2.getBytes(StandardCharsets.UTF_8);
            int putLen2 = 4 + 4 + reqIdBytes2.length + 4 + 4 + keyBytes.length + data2.length;

            writeInt(out, putLen2);
            writeInt(out, 1); // PUT
            writeString(out, reqId2);
            writeInt(out, partition);
            writeString(out, key);
            out.write(data2);
            out.flush();
            int status2 = in.read();
            assertEquals(1, status2, "Second PUT should return success (1)");

            // GET (should return second value)
            int getLen = 4 + 4 + 4 + keyBytes.length;
            writeInt(out, getLen);
            writeInt(out, 6); // GET
            writeInt(out, partition);
            writeString(out, key);
            out.flush();

            int found = in.read();
            assertEquals(1, found, "GET after overwrite should return found (1)");
            byte[] responseData = in.readNBytes(data2.length);
            assertEquals("SecondValue", new String(responseData, StandardCharsets.UTF_8));
        }
    }

    private void writeInt(OutputStream out, int v) throws IOException {
        out.write(ByteBuffer.allocate(4).putInt(v).array());
    }

    private void writeString(OutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        writeInt(out, b.length);
        out.write(b);
    }
}