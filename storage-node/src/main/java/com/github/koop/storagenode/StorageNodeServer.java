package com.github.koop.storagenode;

import com.github.koop.common.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageNodeServer {

    private final int port;
    private final StorageNode storageNode;
    private Server grpcServer;

    private static final Logger logger = LogManager.getLogger(StorageNodeServer.class);
    private static final int CHUNK_SIZE = 1 << 20;   // 1 MB

    public StorageNodeServer(int port, Path dir) {
        this.port = port;
        this.storageNode = new StorageNode(dir);
    }

    public static void main(String[] args) {
        String envPort = System.getenv("PORT");
        String envDir  = System.getenv("STORAGE_DIR");

        int  port        = (envPort != null) ? Integer.parseInt(envPort) : 8080;
        Path storagePath = Path.of((envDir != null) ? envDir : "./storage");

        logger.info("Starting StorageNodeServer with port={} and storagePath={}", port, storagePath);

        try {
            java.nio.file.Files.createDirectories(storagePath);
        } catch (IOException e) {
            logger.error("Failed to create storage directory: " + storagePath);
            System.exit(1);
        }

        StorageNodeServer server = new StorageNodeServer(port, storagePath);

        logger.info("Storage Node starting on port: " + port);
        logger.info("Storage directory: " + storagePath.toAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server...");
            server.stop();
        }));

        server.start();
    }

    // -------------------------------------------------------------------
    //  Lifecycle
    // -------------------------------------------------------------------

    public void start() {
        try {
            grpcServer = ServerBuilder.forPort(port)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .addService(new StorageNodeGrpcService())
                    .build()
                    .start();

            logger.info("Storage Node gRPC server started on port {}", port);
            grpcServer.awaitTermination();
        } catch (IOException e) {
            logger.error("Failed to start gRPC server", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
    }

    // -------------------------------------------------------------------
    //  BlockingQueue-backed InputStream.
    //
    //  Unlike PipedInputStream this works regardless of how many threads
    //  write into the queue — perfect for gRPC callbacks that may arrive
    //  on different virtual threads.
    // -------------------------------------------------------------------

    private static final ByteString POISON = ByteString.EMPTY;

    private static final class QueueInputStream extends InputStream {

        private final BlockingQueue<ByteString> queue;
        private byte[] current;
        private int    pos;

        QueueInputStream(BlockingQueue<ByteString> queue) {
            this.queue = queue;
        }

        @Override
        public int read() throws IOException {
            if (!ensure()) return -1;
            return current[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (!ensure()) return -1;
            int n = Math.min(len, current.length - pos);
            System.arraycopy(current, pos, b, off, n);
            pos += n;
            return n;
        }

        private boolean ensure() throws IOException {
            while (current == null || pos >= current.length) {
                ByteString next;
                try {
                    next = queue.take();      // parks virtual thread, never pins
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while reading from queue", e);
                }
                if (next == POISON) return false;   // end-of-stream sentinel
                current = next.toByteArray();
                pos = 0;
            }
            return true;
        }
    }

    // -------------------------------------------------------------------
    //  gRPC service implementation
    // -------------------------------------------------------------------

    private class StorageNodeGrpcService
            extends StorageNodeServiceGrpc.StorageNodeServiceImplBase {

        // ---- PUT (client-streaming) ----

        @Override
        public StreamObserver<PutShardRequest> putShard(
                StreamObserver<PutShardResponse> responseObserver) {

            return new StreamObserver<>() {

                private final BlockingQueue<ByteString> dataQueue = new LinkedBlockingQueue<>();
                private volatile Thread   storeThread;
                private volatile boolean  storeSuccess;
                private volatile Throwable storeError;
                private volatile String   reqId;

                @Override
                public void onNext(PutShardRequest request) {
                    switch (request.getPayloadCase()) {
                        case METADATA -> {
                            var meta = request.getMetadata();
                            reqId = meta.getRequestId();
                            logger.debug("PUT metadata: reqId={}, partition={}, key={}, len={}",
                                    reqId, meta.getPartition(), meta.getKey(),
                                    meta.getPayloadLength());

                            InputStream queueStream = new QueueInputStream(dataQueue);
                            ReadableByteChannel channel = Channels.newChannel(queueStream);

                            storeThread = Thread.startVirtualThread(() -> {
                                try {
                                    storageNode.store(
                                            meta.getPartition(),
                                            meta.getRequestId(),
                                            meta.getKey(),
                                            channel,
                                            meta.getPayloadLength());
                                    storeSuccess = true;
                                } catch (IOException e) {
                                    storeError = e;
                                    logger.debug("Store failed for reqId={}: {}",
                                            reqId, e.getMessage());
                                }
                            });
                        }
                        case CHUNK -> {
                            try {
                                dataQueue.put(request.getChunk());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        default -> logger.warn("Unknown PutShardRequest payload case");
                    }
                }

                @Override
                public void onError(Throwable t) {
                    dataQueue.offer(POISON);          // unblock store thread
                }

                @Override
                public void onCompleted() {
                    try {
                        dataQueue.put(POISON);        // signal end-of-stream
                        if (storeThread != null) storeThread.join();
                    } catch (Exception ignored) {}

                    responseObserver.onNext(PutShardResponse.newBuilder()
                            .setSuccess(storeSuccess && storeError == null)
                            .build());
                    responseObserver.onCompleted();
                }
            };
        }

        // ---- GET (server-streaming) ----

        @Override
        public void getShard(GetShardRequest request,
                             StreamObserver<GetShardResponse> responseObserver) {
            try {
                int    partition = request.getPartition();
                String key       = request.getKey();
                logger.debug("GET: partition={}, key={}", partition, key);

                var data = storageNode.retrieve(partition, key);

                if (data.isEmpty()) {
                    responseObserver.onNext(GetShardResponse.newBuilder()
                            .setMetadata(GetShardMetadata.newBuilder()
                                    .setFound(false).build())
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                try (FileChannel fc = data.get()) {
                    responseObserver.onNext(GetShardResponse.newBuilder()
                            .setMetadata(GetShardMetadata.newBuilder()
                                    .setFound(true).build())
                            .build());

                    ByteBuffer buf = ByteBuffer.allocate(CHUNK_SIZE);
                    while (true) {
                        buf.clear();
                        int bytesRead = fc.read(buf);
                        if (bytesRead == -1) break;
                        buf.flip();
                        responseObserver.onNext(GetShardResponse.newBuilder()
                                .setChunk(ByteString.copyFrom(buf))
                                .build());
                    }
                }

                responseObserver.onCompleted();
            } catch (IOException e) {
                responseObserver.onError(
                        Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        }

        // ---- DELETE (unary) ----

        @Override
        public void deleteShard(DeleteShardRequest request,
                                StreamObserver<DeleteShardResponse> responseObserver) {
            try {
                int    partition = request.getPartition();
                String key       = request.getKey();
                logger.debug("DELETE: partition={}, key={}", partition, key);

                boolean result = storageNode.delete(partition, key);

                responseObserver.onNext(DeleteShardResponse.newBuilder()
                        .setSuccess(result)
                        .build());
                responseObserver.onCompleted();
            } catch (IOException e) {
                responseObserver.onError(
                        Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        }
    }
}
