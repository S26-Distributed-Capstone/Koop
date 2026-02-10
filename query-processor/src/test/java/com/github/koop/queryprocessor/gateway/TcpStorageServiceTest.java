package com.github.koop.queryprocessor.gateway;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.koop.queryprocessor.gateway.StorageServices.TcpStorageService;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TcpStorageServiceTest {

    private TcpStorageService service;
    private ExecutorService serverExecutor;
    private ServerSocket serverSocket;
    private int ephemeralPort;

    @BeforeEach
    void setUp() throws IOException {
        // Use Port 0 to let the OS pick a free port (Avoids VS Code collisions)
        serverSocket = new ServerSocket(0);
        ephemeralPort = serverSocket.getLocalPort();
        System.out.println("TEST: Mock Server listening on port " + ephemeralPort);

        service = new TcpStorageService("localhost", ephemeralPort);
        serverExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        serverExecutor.shutdownNow();
    }

    @Test
    void testPutProtocolCompliance() throws Exception {
        // 1. Start the Mock Server in a separate thread
        Future<?> serverTask = serverExecutor.submit(() -> {
            try (Socket client = serverSocket.accept()) {
                // Set timeout to fail fast if data is missing (2 seconds)
                client.setSoTimeout(2000);
                
                DataInputStream in = new DataInputStream(client.getInputStream());
                DataOutputStream out = new DataOutputStream(client.getOutputStream());

                System.out.println("SERVER: Client connected");

                // --- VERIFY PROTOCOL ---
                
                // A. Frame Length
                long frameLength = in.readLong();
                System.out.println("SERVER: Read Frame Length: " + frameLength);
                assertTrue(frameLength > 0, "Should send valid frame length");

                long bytesReadFromPayload = 0;

                // B. Opcode
                int opcode = in.readInt();
                bytesReadFromPayload += 4;
                System.out.println("SERVER: Read Opcode: " + opcode);
                assertEquals(1, opcode, "Opcode should be 1 (PUT)");

                // C. Request ID
                int reqIdLen = in.readInt();
                bytesReadFromPayload += 4;
                byte[] reqIdBytes = in.readNBytes(reqIdLen);
                bytesReadFromPayload += reqIdLen;
                String reqId = new String(reqIdBytes, StandardCharsets.UTF_8);
                System.out.println("SERVER: Read ReqID: " + reqId);
                
                // D. Partition
                int partition = in.readInt();
                bytesReadFromPayload += 4;
                System.out.println("SERVER: Read Partition: " + partition);

                // E. Key
                int keyLen = in.readInt();
                bytesReadFromPayload += 4;
                byte[] keyBytes = in.readNBytes(keyLen);
                bytesReadFromPayload += keyLen;
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                System.out.println("SERVER: Read Key: " + key);
                assertEquals("my-test-key", key);

                // F. Data
                // Calculate EXACT remaining size
                int dataSize = (int) (frameLength - bytesReadFromPayload);
                System.out.println("SERVER: Expecting Data Bytes: " + dataSize);
                
                byte[] data = in.readNBytes(dataSize);
                String content = new String(data, StandardCharsets.UTF_8);
                System.out.println("SERVER: Read Content: " + content);
                assertEquals("Hello World", content);

                // --- SEND RESPONSE ---
                System.out.println("SERVER: Sending Success Response");
                out.writeLong(1); 
                out.write(1); // Success
                out.flush();
                
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        // 2. Run Client Code
        String key = "my-test-key";
        String content = "Hello World";
        ByteArrayInputStream data = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        
        System.out.println("CLIENT: Sending PUT request...");
        service.putObject("my-bucket", key, data);
        System.out.println("CLIENT: PUT request finished");

        // Ensure server finished cleanly
        serverTask.get(2, TimeUnit.SECONDS);
    }

    @Test
    void testGetProtocolCompliance() throws Exception {
        Future<?> serverTask = serverExecutor.submit(() -> {
            try (Socket client = serverSocket.accept()) {
                client.setSoTimeout(2000);
                DataInputStream in = new DataInputStream(client.getInputStream());
                DataOutputStream out = new DataOutputStream(client.getOutputStream());

                System.out.println("SERVER: Client connected for GET");

                // --- VERIFY REQUEST ---
                long frameLength = in.readLong();
                int opcode = in.readInt();
                System.out.println("SERVER: Opcode: " + opcode);
                assertEquals(6, opcode, "Opcode should be 6 (GET)");
                
                in.readInt(); // Skip partition
                
                int keyLen = in.readInt();
                String key = new String(in.readNBytes(keyLen), StandardCharsets.UTF_8);
                System.out.println("SERVER: Key: " + key);
                assertEquals("get-key", key);

                // --- SEND RESPONSE ---
                byte[] payload = "FoundIt".getBytes(StandardCharsets.UTF_8);
                out.writeLong(1 + payload.length);
                out.write(1); // Status: Success
                out.write(payload);
                out.flush();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        System.out.println("CLIENT: Sending GET request...");
        InputStream result = service.getObject("bucket", "get-key");
        assertNotNull(result);
        String content = new String(result.readAllBytes(), StandardCharsets.UTF_8);
        System.out.println("CLIENT: Received Content: " + content);
        assertEquals("FoundIt", content);
        result.close();
        
        serverTask.get(2, TimeUnit.SECONDS);
    }
}