package edu.yu.cs.com4020.com.koop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

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

    @Override
    public void putObject(String bucket, String key, InputStream data) throws Exception {
        //TODO: Update to use the new protocol format
        throw new UnsupportedOperationException("PUT not implemented yet");
    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        //TODO: Update to use the new protocol format
        throw new UnsupportedOperationException("GET not implemented yet");
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        //TODO: Update to use the new protocol format
        throw new UnsupportedOperationException("DELETE not implemented yet");
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