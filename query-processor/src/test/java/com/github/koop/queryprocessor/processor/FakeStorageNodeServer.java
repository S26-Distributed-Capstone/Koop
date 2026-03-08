package com.github.koop.queryprocessor.processor;

import com.github.koop.common.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fake gRPC storage-node server for unit testing.
 * Stores shards in memory and honours the {@code enabled} flag to simulate
 * node failures.
 */
public final class FakeStorageNodeServer implements Closeable {

    private static final int CHUNK_SIZE = 1 << 20;  // 1 MB

    private final Server server;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private volatile boolean enabled = true;

    private static final Logger logger = LogManager.getLogger(FakeStorageNodeServer.class);

    public FakeStorageNodeServer() throws IOException {
        this.server = ServerBuilder.forPort(0)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .maxInboundMessageSize(64 * 1024 * 1024)
                .addService(new FakeService())
                .build()
                .start();
    }

    public InetSocketAddress address() {
        return new InetSocketAddress("127.0.0.1", server.getPort());
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public void close() throws IOException {
        server.shutdownNow();
    }

    // -------------------------------------------------------
    //  Inner gRPC service
    // -------------------------------------------------------

    private class FakeService
            extends StorageNodeServiceGrpc.StorageNodeServiceImplBase {

        // ---- PUT (client-streaming) ----

        @Override
        public StreamObserver<PutShardRequest> putShard(
                StreamObserver<PutShardResponse> responseObserver) {

            if (!enabled) {
                responseObserver.onError(
                        Status.UNAVAILABLE.withDescription("Node disabled").asRuntimeException());
                return noOpObserver();
            }

            return new StreamObserver<>() {
                private int partition;
                private String key;
                private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

                @Override
                public void onNext(PutShardRequest request) {
                    switch (request.getPayloadCase()) {
                        case METADATA -> {
                            partition = request.getMetadata().getPartition();
                            key       = request.getMetadata().getKey();
                        }
                        case CHUNK -> {
                            try { request.getChunk().writeTo(buf); }
                            catch (IOException ignored) {}
                        }
                        default -> {}
                    }
                }

                @Override public void onError(Throwable t) {}

                @Override
                public void onCompleted() {
                    store.put(mapKey(partition, key), buf.toByteArray());
                    responseObserver.onNext(PutShardResponse.newBuilder()
                            .setSuccess(true).build());
                    responseObserver.onCompleted();
                }
            };
        }

        // ---- GET (server-streaming) ----

        @Override
        public void getShard(GetShardRequest request,
                             StreamObserver<GetShardResponse> responseObserver) {

            if (!enabled) {
                responseObserver.onError(
                        Status.UNAVAILABLE.withDescription("Node disabled").asRuntimeException());
                return;
            }

            byte[] data = store.get(mapKey(request.getPartition(), request.getKey()));

            if (data == null) {
                responseObserver.onNext(GetShardResponse.newBuilder()
                        .setMetadata(GetShardMetadata.newBuilder()
                                .setFound(false).build())
                        .build());
            } else {
                responseObserver.onNext(GetShardResponse.newBuilder()
                        .setMetadata(GetShardMetadata.newBuilder()
                                .setFound(true).build())
                        .build());

                int offset = 0;
                while (offset < data.length) {
                    int len = Math.min(CHUNK_SIZE, data.length - offset);
                    responseObserver.onNext(GetShardResponse.newBuilder()
                            .setChunk(ByteString.copyFrom(data, offset, len))
                            .build());
                    offset += len;
                }
            }
            responseObserver.onCompleted();
        }

        // ---- DELETE (unary) ----

        @Override
        public void deleteShard(DeleteShardRequest request,
                                StreamObserver<DeleteShardResponse> responseObserver) {

            if (!enabled) {
                responseObserver.onError(
                        Status.UNAVAILABLE.withDescription("Node disabled").asRuntimeException());
                return;
            }

            store.remove(mapKey(request.getPartition(), request.getKey()));
            responseObserver.onNext(DeleteShardResponse.newBuilder()
                    .setSuccess(true).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------

    private static String mapKey(int partition, String key) {
        return partition + "|" + key;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamObserver<T> noOpObserver() {
        return (StreamObserver<T>) NO_OP;
    }

    private static final StreamObserver<?> NO_OP = new StreamObserver<>() {
        @Override public void onNext(Object v)     {}
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted()        {}
    };
}
