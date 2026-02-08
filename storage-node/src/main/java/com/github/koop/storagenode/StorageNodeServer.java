package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class StorageNodeServer {

    private int port;
    private final Map<Integer, Handler> handlers;
    private final StorageNode storageNode;

    private static final int OPCODE_PUT = 1;
    private static final int OPCODE_GET = 6;
    private static final int OPCODE_DELETE = 2;

    private ServerSocketChannel serverSocketChannel;

    public StorageNodeServer(int port, Path dir) {
        this.port = port;
        this.handlers = new ConcurrentHashMap<>();
        this.storageNode = new StorageNode(dir);
        registerHandlers();
    }

    private void registerHandlers() {
        this.handlers.put(OPCODE_PUT, this::handlePut);
        this.handlers.put(OPCODE_GET, this::handleGet);
        this.handlers.put(OPCODE_DELETE, this::handleDelete);
    }

    private ByteBuffer succeeded(boolean successful) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) (successful ? 1 : 0));
        buffer.flip();
        return buffer;
    }

    private ByteBuffer length(long length) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(length);
        buffer.flip();
        return buffer;
    }

    // --- Handlers ---

    protected void handlePut(SocketChannel socketChannel, long length) throws IOException {
        var reqIdBytes = readBytes(socketChannel);
        var reqId = new String(reqIdBytes);
        var partition = readInt(socketChannel);
        var keyBytes = readBytes(socketChannel);
        var key = new String(keyBytes);
        // Calculate payload: Total frame length - (Opcode=4 is already subtracted)
        // We must subtract: ReqIDLen(4) + ReqID + Partition(4) + KeyLen(4) + Key
        long headerOverhead = 4L + reqIdBytes.length + 4L + 4L + keyBytes.length;
        long payloadLength = length - headerOverhead;

        // Use the updated StorageNode API (accepts ReadableByteChannel)
        this.storageNode.store(partition, reqId, key, socketChannel, payloadLength);
        
        socketChannel.write(length(1));
        socketChannel.write(succeeded(true));
    }

    protected void handleGet(SocketChannel socketChannel, long length) throws IOException {
        var partition = readInt(socketChannel);
        var key = readString(socketChannel);
        var data = this.storageNode.retrieve(partition, key);
        
        if (data.isEmpty()) {
            socketChannel.write(length(1));
            socketChannel.write(succeeded(false)); // not found
        } else {
            try (var dataChannel = data.get()) {
                var size = dataChannel.size();
                var responseLen = 1 + size; // 1 byte success flag + data
                socketChannel.write(length(responseLen));
                socketChannel.write(succeeded(true));
                transferAll(dataChannel, socketChannel);
                dataChannel.close(); // Ensure channel is closed after transfer
            }
        }
    }

    protected void handleDelete(SocketChannel socketChannel, long length) throws IOException {
        var partition = readInt(socketChannel);
        var key = readString(socketChannel);
        var result = this.storageNode.delete(partition, key);
        socketChannel.write(length(1));
        socketChannel.write(succeeded(result));
    }

    // --- Server Lifecycle ---

    public void start() {
        var executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.bind(new InetSocketAddress(port));
            
            while (this.serverSocketChannel.isOpen() && !Thread.currentThread().isInterrupted()) {
                SocketChannel clientChannel;
                try {
                    clientChannel = this.serverSocketChannel.accept();
                } catch (ClosedByInterruptException e) {
                    // Server is stopping (likely triggered by test tearDown)
                    break;
                }

                if (!this.serverSocketChannel.isOpen()) {
                    break;
                }
                
                executor.submit(() -> {
                    try (clientChannel) {
                        while (clientChannel.isConnected()) {
                            // 1. Read Frame Length (Check for EOF here)
                            ByteBuffer lenBuf = ByteBuffer.allocate(8);
                            int read = clientChannel.read(lenBuf);
                            
                            // If client disconnected gracefully, read returns -1
                            if (read == -1) {
                                break; 
                            }
                            
                            // If we got partial bytes, finish reading the 8-byte header
                            while (lenBuf.hasRemaining()) {
                                if (clientChannel.read(lenBuf) == -1) {
                                    throw new EOFException("Unexpected EOF inside length header");
                                }
                            }
                            lenBuf.flip();
                            long length = lenBuf.getLong();
                            
                            if (length <= 0) break;

                            // 2. Read Opcode
                            int opcode = readInt(clientChannel);
                            
                            // 3. Dispatch
                            var handler = this.handlers.get(opcode);
                            if (handler != null) {
                                // subtract 4 bytes (Opcode) from the remaining length
                                handler.handle(clientChannel, length - 4);
                            } else {
                                System.err.println("Unknown opcode: " + opcode);
                                break; // disconnect invalid client
                            }
                        }
                    } catch (EOFException e) {
                        // Client closed connection during a read (Normal behavior)
                    } catch (IOException e) {
                        // Only print real errors (not connection resets during shutdown)
                        if (e.getMessage() != null && !e.getMessage().contains("Connection reset")) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (IOException e) {
            // Check if we are stopping before printing stack trace
            if (this.serverSocketChannel != null && this.serverSocketChannel.isOpen()) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        if (this.serverSocketChannel != null && this.serverSocketChannel.isOpen()) {
            try {
                this.serverSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // --- Helpers ---

    private void readFully(SocketChannel sc, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = sc.read(buf);
            if (n == -1) {
                throw new EOFException("Unexpected EOF");
            }
        }
    }

    private long transferAll(FileChannel src, WritableByteChannel dest) throws IOException {
        long transferred = 0;
        long count = src.size();
        while (transferred < count) {
            long n = src.transferTo(transferred, count - transferred, dest);
            if (n <= 0) {
                break;
            }
            transferred += n;
        }
        return transferred;
    }

    private int readInt(SocketChannel in) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        readFully(in, buf);
        buf.flip();
        return buf.getInt();
    }

    private byte[] readBytes(SocketChannel in) throws IOException {
        var length = readInt(in);
        ByteBuffer buf = ByteBuffer.allocate(length);
        readFully(in, buf);
        buf.flip();
        return buf.array();
    }

    private String readString(SocketChannel in) throws IOException {
        return new String(readBytes(in));
    }
}