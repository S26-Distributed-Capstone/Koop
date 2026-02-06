package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * The {@code StorageNodeServer} class implements a simple server for handling storage operations
 * such as PUT, GET, and DELETE on a {@link StorageNode}. It listens for incoming client connections
 * and dispatches requests to appropriate handlers based on operation codes (opcodes).
 * <p>
 * The server uses a thread-per-task executor with virtual threads for handling concurrent client connections.
 * Handlers for each operation are registered in a concurrent map and invoked based on the opcode received from the client.
 * <p>
 * Data is read from the client using a simple protocol where integers and strings are read from the input stream.
 * 
 */
public class StorageNodeServer {

    private int port;
    private final Map<Integer, Handler> handlers;

    private final StorageNode storageNode;

    private static final int OPCODE_PUT = 1;
    private static final int OPCODE_GET = 6;
    private static final int OPCODE_DELETE = 2;

    public StorageNodeServer(int port) {
        this.port = port;
        this.handlers = new ConcurrentHashMap<>();
        this.storageNode = new StorageNode(Paths.get("data"));
        registerHandlers();
    }

    private void registerHandlers() {
        this.handlers.put(OPCODE_PUT, this::handlePut);
        this.handlers.put(OPCODE_GET, this::handleGet);
        this.handlers.put(OPCODE_DELETE, this::handleDelete);
    }

    protected void handlePut(Socket socket, int length) throws IOException {
        var in = socket.getInputStream();
        var reqId = readString(in);
        var partition = readInt(in);
        var key = readString(in);
        int payloadLength = length - 4 - reqId.getBytes().length - 4 - key.getBytes().length - 4; // subtract lengths of reqId, partition, key and their length prefixes
        this.storageNode.store(partition, reqId, key, in,payloadLength);
        socket.getOutputStream().write(1); // success
    }

    protected void handleGet(Socket socket, int length) throws IOException {
        var in = socket.getInputStream();
        var partition = readInt(in);
        var key = readString(in);
        var data = this.storageNode.retrieve(partition, key);
        if(data.isEmpty()){
            socket.getOutputStream().write(0); // not found
        } else {
            socket.getOutputStream().write(1); // found
            try (var dataStream = data.get()) {
                dataStream.transferTo(socket.getOutputStream());
            }
        }
    }

    protected void handleDelete(Socket socket, int length) throws IOException {
        var in = socket.getInputStream();
        var partition = readInt(in);
        var key = readString(in);
        var result = this.storageNode.delete(partition, key);
        socket.getOutputStream().write(result ? 1 : 0);
    }

    public void start() {
        var executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        try (var serverSocket = new ServerSocket(port)) {
            while (true) {
                var clientSocket = serverSocket.accept();
                executor.submit(() -> {
                    try (clientSocket) {
                        InputStream in = clientSocket.getInputStream();
                        while (clientSocket.isConnected()) {
                            int length = readInt(in);
                            if (length <= 0) {
                                break;
                            }
                            int opcode = readInt(in);
                            var handler = this.handlers.get(opcode);
                            //subtract 4 for opcode
                            handler.handle(clientSocket, length-4);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int readInt(InputStream in) {
        try {
            byte[] buf = new byte[4];
            int bytesRead = in.readNBytes(buf, 0, 4);
            if (bytesRead < 4) {
                throw new EOFException("Not enough bytes to read an int");
            }
            return ((buf[0] & 0xFF) << 24) |
                    ((buf[1] & 0xFF) << 16) |
                    ((buf[2] & 0xFF) << 8) |
                    (buf[3] & 0xFF);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readBytes(InputStream in) {
        try {
            var length = readInt(in);
            byte[] readBytes = new byte[length];
            int bytesRead = in.readNBytes(readBytes, 0, length);
            if (bytesRead < length) {
                throw new EOFException("Expected " + length + " bytes but got " + bytesRead);
            }
            return readBytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String readString(InputStream in) {
        return new String(readBytes(in));
    }

}