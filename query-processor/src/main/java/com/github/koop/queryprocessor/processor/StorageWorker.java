package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.github.koop.common.erasure.ErasureCoder.TOTAL;

/**
 * Erasure-coded storage client that fans out to 9 storage nodes over gRPC.
 *
 * <p>Every blocking call in this class runs on a virtual thread, so the
 * code reads like straight-line blocking I/O while never pinning a
 * platform thread.
 */
public final class StorageWorker {

    private static final int  RPC_DEADLINE_SECONDS = 30;
    private static final int  CHUNK_SIZE           = 1 << 20;   // 1 MB

    private final List<ManagedChannel> set1;
    private final List<ManagedChannel> set2;
    private final List<ManagedChannel> set3;

    private static final Logger logger = LogManager.getLogger(StorageWorker.class);

    private final ExecutorService executor;

    public StorageWorker() {
        set1 = List.of();
        set2 = List.of();
        set3 = List.of();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public StorageWorker(List<InetSocketAddress> set1Addrs,
                         List<InetSocketAddress> set2Addrs,
                         List<InetSocketAddress> set3Addrs) {
        if (set1Addrs.size() != TOTAL || set2Addrs.size() != TOTAL || set3Addrs.size() != TOTAL) {
            throw new IllegalArgumentException("Each set must have exactly " + TOTAL + " nodes");
        }
        this.set1 = buildChannels(set1Addrs);
        this.set2 = buildChannels(set2Addrs);
        this.set3 = buildChannels(set3Addrs);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    // -------------------------------------------------------
    //  PUT
    // -------------------------------------------------------

    public boolean put(UUID requestID, String bucket, String key,
                       long length, InputStream data) throws IOException {

        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");
        if (data == null)      throw new IllegalArgumentException("data is null");
        if (length < 0)        throw new IllegalArgumentException("length < 0");

        String storageKey = bucket + "-" + key;
        int[]  routing    = ErasureRouting.setForKey(storageKey);
        int    setNum     = routing[0];
        int    partition  = routing[1];

        List<ManagedChannel> channels = channelsForKey(setNum);

        InputStream[] shardStreams = ErasureCoder.shard(data, length);

        long stripeDataBytes = (long) ErasureCoder.K * ErasureCoder.SHARD_SIZE;
        long numStripes      = (length + stripeDataBytes - 1) / stripeDataBytes;
        long shardPayload    = 8L + numStripes * ErasureCoder.SHARD_SIZE;

        // One virtual-thread task per shard — drains pipes concurrently (required
        // by ErasureCoder) and streams each shard to its storage node over gRPC.
        List<Callable<Boolean>> tasks = new ArrayList<>(TOTAL);

        for (int i = 0; i < TOTAL; i++) {
            final int idx = i;
            tasks.add(() -> {
                try {
                    var asyncStub = StorageNodeServiceGrpc.newStub(channels.get(idx));
                    CompletableFuture<Boolean> result = new CompletableFuture<>();

                    StreamObserver<PutShardRequest> reqStream = asyncStub.putShard(
                            new StreamObserver<>() {
                                @Override public void onNext(PutShardResponse r) { result.complete(r.getSuccess()); }
                                @Override public void onError(Throwable t)      {
                                    logger.trace("PUT shard {} gRPC error: {}", idx, t.getMessage());
                                    result.complete(false);
                                }
                                @Override public void onCompleted()             { if (!result.isDone()) result.complete(false); }
                            });

                    // 1) metadata
                    reqStream.onNext(PutShardRequest.newBuilder()
                            .setMetadata(PutShardMetadata.newBuilder()
                                    .setRequestId(requestID.toString())
                                    .setPartition(partition)
                                    .setKey(storageKey)
                                    .setPayloadLength(shardPayload)
                                    .build())
                            .build());

                    // 2) stream shard data in chunks
                    byte[] buf = new byte[CHUNK_SIZE];
                    int n;
                    while ((n = shardStreams[idx].read(buf)) != -1) {
                        reqStream.onNext(PutShardRequest.newBuilder()
                                .setChunk(ByteString.copyFrom(buf, 0, n))
                                .build());
                    }
                    reqStream.onCompleted();

                    // 3) block virtual thread until the server responds
                    return result.get(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);

                } catch (Exception e) {
                    logger.trace("PUT shard {} failed: {}", idx, e.getMessage());
                    return false;
                }
            });
        }

        long numWritten;
        try {
            numWritten = executor.invokeAll(tasks).stream()
                    .map(f -> { try { return f.get(); } catch (Exception e) { return false; } })
                    .filter(ok -> ok)
                    .count();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        logger.trace("PUT completed, {} shards written successfully", numWritten);
        return numWritten >= ErasureCoder.K;
    }

    // -------------------------------------------------------
    //  GET
    // -------------------------------------------------------

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[]  routing    = ErasureRouting.setForKey(storageKey);
        int    setNum     = routing[0];
        int    partition  = routing[1];

        List<ManagedChannel> channels = channelsForKey(setNum);

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream  pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, channels, pos);
            } catch (Exception e) {
                try { pos.close(); } catch (IOException ignored) {}
            }
        });

        return pis;
    }

    // -------------------------------------------------------
    //  DELETE
    // -------------------------------------------------------

    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[]  routing    = ErasureRouting.setForKey(storageKey);
        int    setNum     = routing[0];
        int    partition  = routing[1];

        List<ManagedChannel> channels = channelsForKey(setNum);
        List<Callable<Boolean>> tasks = new ArrayList<>(TOTAL);

        for (int i = 0; i < TOTAL; i++) {
            final int idx = i;
            tasks.add(() -> {
                try {
                    var stub = StorageNodeServiceGrpc.newBlockingStub(channels.get(idx))
                            .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);

                    var resp = stub.deleteShard(DeleteShardRequest.newBuilder()
                            .setPartition(partition)
                            .setKey(storageKey)
                            .build());

                    return resp.getSuccess();
                } catch (Exception e) {
                    logger.warn("Delete failed for node {}: {}", idx, e.getMessage());
                    return false;
                }
            });
        }

        try {
            return executor.invokeAll(tasks).stream().allMatch(f -> {
                try { return f.get(); } catch (Exception e) { return false; }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void shutdown() {
        executor.shutdownNow();
        shutdownChannels(set1);
        shutdownChannels(set2);
        shutdownChannels(set3);
    }

    // -------------------------------------------------------
    //  Private helpers
    // -------------------------------------------------------

    /**
     * Contacts all 9 nodes via blocking server-streaming gRPC, collects the
     * available shard streams, then feeds them into {@link ErasureCoder#reconstruct}.
     *
     * <p>Every {@code Iterator.next()} call is a blocking call — on a virtual
     * thread this simply parks without pinning a platform thread.
     */
    private void streamReconstruct(int partition, String storageKey,
                                   List<ManagedChannel> channels,
                                   OutputStream out) throws IOException {

        InputStream[] ins     = new InputStream[TOTAL];
        boolean[]     present = new boolean[TOTAL];

        for (int i = 0; i < TOTAL; i++) {
            try {
                var stub = StorageNodeServiceGrpc.newBlockingStub(channels.get(i))
                        .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);

                var responses = stub.getShard(GetShardRequest.newBuilder()
                        .setPartition(partition)
                        .setKey(storageKey)
                        .build());

                if (!responses.hasNext()) { present[i] = false; continue; }

                var first = responses.next();
                if (!first.hasMetadata() || !first.getMetadata().getFound()) {
                    present[i] = false;
                    continue;
                }

                present[i] = true;

                // Spin up a virtual thread that reads the remaining gRPC response
                // chunks and feeds them into a pipe that ErasureCoder will read.
                PipedOutputStream shardOut = new PipedOutputStream();
                ins[i] = new PipedInputStream(shardOut, 4 * 1024 * 1024);

                final var iter = responses;
                Thread.startVirtualThread(() -> {
                    try (shardOut) {
                        while (iter.hasNext()) {
                            var resp = iter.next();
                            if (resp.hasChunk()) {
                                resp.getChunk().writeTo(shardOut);
                            }
                        }
                    } catch (IOException ignored) {
                        // pipe closed by consumer — normal during failure tolerance
                    }
                });

            } catch (Exception e) {
                present[i] = false;
            }
        }

        int count = 0;
        for (boolean b : present) if (b) count++;
        if (count < ErasureCoder.K)
            throw new IOException("lost too many shards; need " + ErasureCoder.K + ", got " + count);

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present)) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = reconstructed.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }

        out.flush();
    }

    private List<ManagedChannel> channelsForKey(int setNum) {
        return switch (setNum) {
            case 1  -> set1;
            case 2  -> set2;
            default -> set3;
        };
    }

    private static List<ManagedChannel> buildChannels(List<InetSocketAddress> addrs) {
        return addrs.stream()
                .map(a -> ManagedChannelBuilder
                        .forAddress(a.getHostString(), a.getPort())
                        .usePlaintext()
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .build())
                .toList();
    }

    private static void shutdownChannels(List<ManagedChannel> channels) {
        for (ManagedChannel ch : channels) {
            try { ch.shutdownNow(); } catch (Exception ignored) {}
        }
    }
}
