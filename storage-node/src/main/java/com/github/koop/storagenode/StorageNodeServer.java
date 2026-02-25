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
import java.util.logging.Logger;

import com.github.koop.common.messages.ChannelMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.MessageReader;
import com.github.koop.common.messages.Opcode;

public class StorageNodeServer {

    private int port;
    private final Map<Integer, Handler> handlers;
    private final StorageNode storageNode;
    static Logger logger = Logger.getLogger(StorageNodeServer.class.getName());


    private ServerSocketChannel serverSocketChannel;

    public StorageNodeServer(int port, Path dir) {
        this.port = port;
        this.handlers = new ConcurrentHashMap<>();
        this.storageNode = new StorageNode(dir);
        registerHandlers();
    }

    public static void main(String[] args) {
        // 1. Read configuration from Environment Variables
        String envPort = System.getenv("PORT");
        String envDir = System.getenv("STORAGE_DIR");

        // 2. Set defaults if environment variables are missing
        int port = (envPort != null) ? Integer.parseInt(envPort) : 8080;
        Path storagePath = Path.of((envDir != null) ? envDir : "./storage");

        // 3. Ensure the storage directory exists
        try {
            java.nio.file.Files.createDirectories(storagePath);
        } catch (IOException e) {
            System.err.println("Failed to create storage directory: " + storagePath);
            System.exit(1);
        }

        // 4. Initialize and start the server
        StorageNodeServer server = new StorageNodeServer(port, storagePath);

        // Log server startup and configuration
        logger.info("Storage Node starting on port: " + port);
        logger.info("Storage directory: " + storagePath.toAbsolutePath());

        // Add a shutdown hook to close the server gracefully on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.stop();
        }));

        server.start();
    }

    private void registerHandlers() {
        this.handlers.put(Opcode.SN_PUT.getCode(), this::handlePut);
        this.handlers.put(Opcode.SN_GET.getCode(), this::handleGet);
        this.handlers.put(Opcode.SN_DELETE.getCode(), this::handleDelete);
    }

    // --- Handlers ---

    protected void handlePut(SocketChannel socketChannel, MessageReader messageReader) throws IOException {
        var reqId = messageReader.readString();
        var partition = messageReader.readInt();
        var key = messageReader.readString();

        var messageWriter = new MessageBuilder(Opcode.SN_PUT);
        long payloadLength = messageReader.getRemainingLength();

        this.storageNode.store(partition, reqId, key, socketChannel, payloadLength);
        //write success
        messageWriter.writeByte((byte)1);
        messageWriter.writeToChannel(socketChannel);
    }

    protected void handleGet(SocketChannel socketChannel, MessageReader messageReader) throws IOException {
        var partition = messageReader.readInt();
        var key = messageReader.readString();
        var data = this.storageNode.retrieve(partition, key);
        var messageBuilder = new MessageBuilder(Opcode.SN_GET);
        if (data.isEmpty()) {
            messageBuilder.writeByte((byte)0);//not found
            messageBuilder.writeToChannel(socketChannel);
        } else {
            try (var dataChannel = data.get()) {
                var size = dataChannel.size();
                messageBuilder.writeByte((byte)1);
                messageBuilder.writeLargePayload(size, dataChannel);
                messageBuilder.writeToChannel(socketChannel);
                dataChannel.close(); // Ensure channel is closed after transfer
            }
        }
    }

    protected void handleDelete(SocketChannel socketChannel, MessageReader messageReader) throws IOException {
        var partition = messageReader.readInt();
        var key = messageReader.readString();
        var result = this.storageNode.delete(partition, key);
        var messageWriter = new MessageBuilder(Opcode.SN_DELETE);
        messageWriter.writeByte((byte)(result ? 1 : 0));
        messageWriter.writeToChannel(socketChannel);
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
                    break;
                }

                if (!this.serverSocketChannel.isOpen()) {
                    break;
                }

                executor.submit(() -> {
                    try (clientChannel) {
                        while (clientChannel.isConnected()) {
                            var messageReader = new ChannelMessageReader(clientChannel);
                            var length = messageReader.getRemainingLength();
                            if (length <= 0) break;
                            var opcode = messageReader.getOpcode();
                            // 3. Dispatch
                            var handler = this.handlers.get(opcode);
                            if (handler != null) {
                                // Client's frameLength excludes opcode already.
                                handler.handle(clientChannel, messageReader);
                            } else {
                                System.err.println("Unknown opcode: " + opcode);
                                break;
                            }
                        }
                    } catch (EOFException e) {
                        // normal disconnect
                    } catch (IOException e) {
                        // Ignore standard client disconnect errors
                        if (e.getMessage() != null && 
                           !e.getMessage().contains("Connection reset") && 
                           !e.getMessage().contains("Broken pipe")) {
                            
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (IOException e) {
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
}
