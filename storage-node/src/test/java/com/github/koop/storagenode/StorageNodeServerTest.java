package com.github.koop.storagenode;

import com.github.koop.common.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class StorageNodeServerTest {

    private static final int PORT = 9092;
    private StorageNodeServer server;
    private ExecutorService serverExecutor;
    private ManagedChannel channel;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        server = new StorageNodeServer(PORT, tempDir);
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> server.start());
        Thread.sleep(300);   // wait for gRPC server to bind

        channel = ManagedChannelBuilder.forAddress("localhost", PORT)
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        server.stop();
        serverExecutor.shutdownNow();
    }

    // -------------------------------------------------------
    //  Helpers – blocking wrappers over gRPC streaming RPCs
    // -------------------------------------------------------

    private boolean blockingPut(String reqId, int partition, String key, byte[] data)
            throws Exception {

        var asyncStub = StorageNodeServiceGrpc.newStub(channel);
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        StreamObserver<PutShardRequest> req = asyncStub.putShard(new StreamObserver<>() {
            @Override public void onNext(PutShardResponse r) { result.complete(r.getSuccess()); }
            @Override public void onError(Throwable t)      { result.completeExceptionally(t); }
            @Override public void onCompleted()             { if (!result.isDone()) result.complete(false); }
        });

        req.onNext(PutShardRequest.newBuilder()
                .setMetadata(PutShardMetadata.newBuilder()
                        .setRequestId(reqId)
                        .setPartition(partition)
                        .setKey(key)
                        .setPayloadLength(data.length)
                        .build())
                .build());

        req.onNext(PutShardRequest.newBuilder()
                .setChunk(ByteString.copyFrom(data))
                .build());

        req.onCompleted();
        return result.get(10, TimeUnit.SECONDS);
    }

    /** Returns payload bytes, or null when not found. */
    private byte[] blockingGet(int partition, String key) throws Exception {
        var stub = StorageNodeServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);

        Iterator<GetShardResponse> it = stub.getShard(
                GetShardRequest.newBuilder()
                        .setPartition(partition)
                        .setKey(key)
                        .build());

        if (!it.hasNext()) return null;

        var first = it.next();
        if (!first.hasMetadata() || !first.getMetadata().getFound()) return null;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (it.hasNext()) {
            var resp = it.next();
            if (resp.hasChunk()) resp.getChunk().writeTo(baos);
        }
        return baos.toByteArray();
    }

    private boolean blockingDelete(int partition, String key) {
        var stub = StorageNodeServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);

        var resp = stub.deleteShard(
                DeleteShardRequest.newBuilder()
                        .setPartition(partition)
                        .setKey(key)
                        .build());
        return resp.getSuccess();
    }

    // -------------------------------------------------------
    //  Tests
    // -------------------------------------------------------

    @Test
    void testPutAndGet() throws Exception {
        String reqId = "req-101";
        int partition = 5;
        String key = "my-key";
        byte[] data = "Hello Server".getBytes(StandardCharsets.UTF_8);

        assertTrue(blockingPut(reqId, partition, key, data), "PUT should succeed");

        byte[] got = blockingGet(partition, key);
        assertNotNull(got);
        assertEquals("Hello Server", new String(got, StandardCharsets.UTF_8));
    }

    @Test
    void testDelete() throws Exception {
        String reqId = "del-req";
        int partition = 2;
        String key = "del-key";
        byte[] data = "ToBeDeleted".getBytes(StandardCharsets.UTF_8);

        assertTrue(blockingPut(reqId, partition, key, data), "Setup PUT should succeed");
        assertTrue(blockingDelete(partition, key), "DELETE should succeed");
    }

    @Test
    void testMultipleClientsDifferentKeys() throws Exception {
        int clientCount = 5;
        Thread[] threads = new Thread[clientCount];
        String[] keys = new String[clientCount];

        for (int i = 0; i < clientCount; i++) {
            keys[i] = "key-" + i;
            String value = "value-" + i;
            final int idx = i;

            threads[i] = new Thread(() -> {
                try {
                    assertTrue(blockingPut("mc-" + idx, idx, keys[idx],
                            value.getBytes(StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // Verify each key
        for (int i = 0; i < clientCount; i++) {
            byte[] got = blockingGet(i, keys[i]);
            assertNotNull(got);
            assertEquals("value-" + i, new String(got, StandardCharsets.UTF_8));
        }
    }

    @Test
    void testOverwritePutSameKey() throws Exception {
        int partition = 1;
        String key = "overwrite-key";

        assertTrue(blockingPut("ow-1", partition, key,
                "FirstValue".getBytes(StandardCharsets.UTF_8)));
        assertTrue(blockingPut("ow-2", partition, key,
                "SecondValue".getBytes(StandardCharsets.UTF_8)));

        byte[] got = blockingGet(partition, key);
        assertNotNull(got);
        assertEquals("SecondValue", new String(got, StandardCharsets.UTF_8));
    }

    @Test
    void testConcurrentPutSameKeyConsistency() throws Exception {
        int clientCount = 10;
        String key = "shared-key";
        int partition = 7;
        String[] values = new String[clientCount];
        Thread[] threads = new Thread[clientCount];

        for (int i = 0; i < clientCount; i++) {
            final int idx = i;
            values[idx] = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            threads[idx] = new Thread(() -> {
                try {
                    assertTrue(blockingPut("req-" + idx, partition, key,
                            values[idx].getBytes(StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        byte[] got = blockingGet(partition, key);
        assertNotNull(got);
        String gotStr = new String(got, StandardCharsets.UTF_8);

        boolean matches = false;
        for (String v : values) {
            if (v.equals(gotStr)) { matches = true; break; }
        }
        assertTrue(matches, "GET should return one of the concurrently written values");
    }
}
