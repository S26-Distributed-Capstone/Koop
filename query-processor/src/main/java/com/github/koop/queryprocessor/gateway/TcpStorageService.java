package com.github.koop.queryprocessor.gateway;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * A client implementation for the Storage Node binary protocol.
 * * Protocol Specification:
 * - Transport: TCP
 * - Frame Header: 8 bytes (Long) representing the total length of the payload (including Opcode).
 * - Opcode: 4 bytes (Int).
 * - Strings: 4-byte Integer length prefix followed by UTF-8 bytes.
 * * Opcodes:
 * - PUT (1): Stores data.
 * - DELETE (2): Logically deletes data.
 * - GET (6): Retrieves data.
 */
public class TcpStorageService implements StorageService {

    private final String routerHost;
    private final int routerPort;

    //Protocol Constants
    private static final int OPCODE_PUT = 1;
    private static final int OPCODE_DELETE = 2;
    private static final int OPCODE_GET = 6;

    // Status Constants
    private static final int STATUS_SUCCESS = 1;
    private static final int STATUS_FAILURE = 0;


    public TcpStorageService(String routerHost, int routerPort) {
        this.routerHost = routerHost;
        this.routerPort = routerPort;
    }

    /**
     * Stores an object in the storage node.
     * * Packet Structure:
     * [FrameLen (8)] [Opcode=1 (4)] [ReqID_Str] [Partition (4)] [Key_Str] [Data_Bytes]
     */
    @Override
    public void putObject(String bucket, String key, InputStream data) throws Exception {
        //Data has to be buffered in memory to calculate the frame length before sending
        //per current protocol, but will need to discuss

        // 1. Buffer the Data
        byte[] dataBytes = data.readAllBytes();

        // 2. Prepare Metadata
        String requestId = UUID.randomUUID().toString(); // Unique request ID for tracing/logging
        int partition = getPartition(key); // Simple partitioning based on key hash

        // 3. Calculate Payload Size
        long payloadLength = getStringLength(requestId) + 4 + getStringLength(key) + dataBytes.length;

        //4. Send packet
        try(Socket socket = new Socket(routerHost, routerPort);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // Header (8 bytes)
            writeFrameHeader(out, payloadLength);
            // Opcode (4 bytes)
            out.writeInt(OPCODE_PUT);
            // Payload
            writeString(out, requestId);
            out.writeInt(partition);
            writeString(out, key);
            out.write(dataBytes);
            out.flush();

            if(!readResponseStatus(in)) {
                throw new RuntimeException("Storage Node returned failure status for PUT.");            
            }
        }

    }

    /**
     * Retrieves an object stream from the storage node.
     * * Packet Structure:
     * [FrameLen (8)] [Opcode=6 (4)] [Partition (4)] [Key_Str]
     * * Returns:
     * An open InputStream to the data, or null if not found.
     */
    @SuppressWarnings("resource")
    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        int partition = getPartition(key);
        long payloadLength = 4 + getStringLength(key); // Partition (4 bytes)

        Socket socket = new Socket(routerHost, routerPort);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        try{
            writeFrameHeader(out, payloadLength);
            out.writeInt(OPCODE_GET);
            out.writeInt(partition);
            writeString(out, key);
            out.flush();

            boolean found = readResponseStatus(in);
            if(!found) {
                socket.close();
                return null; // Not found
            }

            return in; // Caller is responsible for closing this stream (and underlying socket)
        }catch(Exception e) {
            socket.close();
            throw e;
        }
    }

    /**
     * Deletes an object from the storage node.
     * * Packet Structure:
     * [FrameLen (8)] [Opcode=2 (4)] [Partition (4)] [Key_Str]
     */
    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        int partition = getPartition(key);
        long payloadLength = 4 + getStringLength(key); // Partition (4 bytes)

        try(Socket socket = new Socket(routerHost, routerPort);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream())) {

            
            writeFrameHeader(out, payloadLength);
            out.writeInt(OPCODE_DELETE);
            out.writeInt(partition);
            writeString(out, key);
            out.flush();

            if(!readResponseStatus(in)) {
                throw new RuntimeException("Storage Node returned failure status for DELETE.");            
            }
        }
    }

    /**
     * Writes the 8-byte frame length header. 
     * The server expects the total frame size (including the Opcode).
     */
    private void writeFrameHeader(DataOutputStream out, long payloadLength) throws IOException {
        // Frame Length = Opcode (4 bytes) + Payload Length
        out.writeLong(4 + payloadLength);
    }

    /**
     * Writes a string in the format: [4-byte Length][Bytes]
     * The server uses UTF-8.
     */
    private void writeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Reads the response header from the server.
     * The server sends: [8-byte Length][1-byte Status]
     * * @return true if status is SUCCESS (1), false otherwise.
     */
    private boolean readResponseStatus(DataInputStream in) throws IOException {
        // 1. Consume the 8-byte response length
        // We don't strictly need the value, but we must read it to advance the stream.
        long length = in.readLong(); 
        
        // 2. Read the 1-byte status
        // The server sends a single byte: 1 for success, 0 for failure.
        // We use readByte() or read() (read returns int 0-255).
        int status = in.read();
        
        return status == STATUS_SUCCESS;
    }

    /**
     * Helper to calculate the byte length of a string when encoded.
     * Needed to calculate the total frame length before sending.
     */
    private int getStringLength(String s) {
        return 4 + s.getBytes(StandardCharsets.UTF_8).length; // 4 bytes for length integer + bytes
    }
    
    private int getPartition(String key) {
        return Math.abs(key.hashCode() % 10);
    }

}