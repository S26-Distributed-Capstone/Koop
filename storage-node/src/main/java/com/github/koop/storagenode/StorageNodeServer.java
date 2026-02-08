package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.google.common.io.ByteStreams;

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

    protected void handlePut(SocketChannel socketChannel, long length) throws IOException {
        var reqIdBytes = readBytes(socketChannel);
        var reqId = new String(reqIdBytes);
        var partition = readInt(socketChannel);
        var keyBytes = readBytes(socketChannel);
        var key = new String(keyBytes);
        long payloadLength = length - 4 - reqIdBytes.length - 4 - keyBytes.length - 4; // subtract lengths of reqId, partition, key and their length prefixes
        this.storageNode.store(partition, reqId, key, socketChannel,payloadLength);
        socketChannel.write(length(1));
        socketChannel.write(succeeded(true));
    }

    protected void handleGet(SocketChannel socketChannel, long length) throws IOException {
        var partition = readInt(socketChannel);
        var key = readString(socketChannel);
        var data = this.storageNode.retrieve(partition, key);
        if(data.isEmpty()){
            socketChannel.write(length(1));
            socketChannel.write(succeeded(false));//not found
        } else {
            try (var dataStream = data.get()) {
                var size = dataStream.size();
                var responseLen = 1 + size; //1 byte for success flag + data size
                socketChannel.write(length(responseLen));
                socketChannel.write(succeeded(true));
                transferAll(dataStream,socketChannel);
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

    public void start() {
        var executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.bind(new InetSocketAddress(port));
            while (this.serverSocketChannel.isOpen() && !Thread.currentThread().isInterrupted()) {
                var clientChannel= this.serverSocketChannel.accept();
                if(!serverSocketChannel.isOpen()){
                    //might have closed on stop()
                    break;
                }
                executor.submit(() -> {
                    try (clientChannel) {
                        while (clientChannel.isConnected()) {
                            long length = readInt(clientChannel);
                            if (length <= 0) {
                                break;
                            }
                            int opcode = readInt(clientChannel);
                            var handler = this.handlers.get(opcode);
                            //subtract 4 for opcode
                            handler.handle(clientChannel, length-4);
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

    public void stop(){
        if(this.serverSocketChannel!=null && this.serverSocketChannel.isOpen()){
            try {
                this.serverSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

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

    private long readLong(SocketChannel in) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(8);
            readFully(in, buf);
            buf.flip();
            return buf.getLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int readInt(SocketChannel in) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(4);
            readFully(in, buf);
            buf.flip();
            return buf.getInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readBytes(SocketChannel in) {
        try {
            var length = readInt(in);
            ByteBuffer buf = ByteBuffer.allocate(length);
            readFully(in, buf);
            buf.flip();
            return buf.array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String readString(SocketChannel in) {
        return new String(readBytes(in));
    }

}