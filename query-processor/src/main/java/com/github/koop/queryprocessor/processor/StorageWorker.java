package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.github.koop.common.erasure.ErasureCoder.TOTAL;

/**
 * Distributes erasure-coded shards to storage nodes via HTTP.
 * Each storage node runs a Javalin server with endpoints:
 * PUT /store/{partition}/{key}?requestId=...
 * GET /store/{partition}/{key}
 * DELETE /store/{partition}/{key}
 */
public final class StorageWorker {

    private final List<InetSocketAddress> set1;
    private final List<InetSocketAddress> set2;
    private final List<InetSocketAddress> set3;

    private static final Logger logger = LogManager.getLogger(StorageWorker.class);

    private final ExecutorService executor;
    private final HttpClient httpClient;

    public StorageWorker() {
        set1 = List.of();
        set2 = List.of();
        set3 = List.of();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3) {
        if (set1.size() != TOTAL || set2.size() != TOTAL || set3.size() != TOTAL) {
            throw new IllegalArgumentException("Each set must have exactly " + TOTAL + " nodes");
        }
        this.set1 = set1;
        this.set2 = set2;
        this.set3 = set3;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");
        if (data == null)
            throw new IllegalArgumentException("data is null");
        if (length < 0)
            throw new IllegalArgumentException("length < 0");

        String storageKey = bucket + "-" + key;
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // Shard the incoming stream — each InputStream carries shard data.
        // IMPORTANT: all shard streams must be drained concurrently because
        // the erasure encoder writes to 9 pipes in a single thread.
        InputStream[] shardStreams = ErasureCoder.shard(data, length);

        List<Callable<Boolean>> tasks = new LinkedList<>();

        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            tasks.add(() -> {
                try {
                    InetSocketAddress node = nodes.get(index);

                    URI uri = URI.create(String.format("http://%s:%d/store/%d/%s?requestId=%s",
                            node.getHostString(), node.getPort(), partition, storageKey, requestID));

                    // REMOVED: byte[] shardData = shardStreams[index].readAllBytes();

                    // ADDED: Stream directly from the InputStream
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(uri)
                            .PUT(HttpRequest.BodyPublishers.ofInputStream(() -> shardStreams[index]))
                            .header("Content-Type", "application/octet-stream")
                            .build();

                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() != 200) {
                        logger.trace("PUT failed for shard {} to node {}: HTTP {}", index, node, response.statusCode());
                        return false;
                    }
                    logger.trace("PUT succeeded for shard {} to node {}", index, node);
                    return true;
                } catch (Exception e) {
                    logger.trace("Exception for shard {}: {}", index, e.getMessage());
                    return false;
                }
            });
        }

        long numWritten;
        try {
            logger.trace("Waiting for all shard uploads to complete");
            numWritten = executor.invokeAll(tasks).stream().map(t -> {
                try {
                    return t.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception in shard upload task {}", e.getMessage());
                    return false;
                }
            }).filter(it -> it).count();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        logger.trace("All shard uploads completed, wrote {} shards successfully", numWritten);
        return numWritten >= ErasureCoder.K;
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, nodes, pos);
            } catch (Exception e) {
                try {
                    pos.close();
                } catch (IOException ignored) {
                }
            }
        });

        return pis;
    }

    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);
        List<Callable<Boolean>> tasks = new LinkedList<>();

        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            tasks.add(() -> {
                try {
                    InetSocketAddress node = nodes.get(index);
                    URI uri = URI.create(String.format("http://%s:%d/store/%d/%s",
                            node.getHostString(), node.getPort(), partition, storageKey));

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(uri)
                            .DELETE()
                            .build();

                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() != 200) {
                        logger.trace("DELETE failed for node {}: HTTP {}", node, response.statusCode());
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    logger.warn("Exception in delete task for node {}: {}", nodes.get(index), e.getMessage());
                    return false;
                }
            });
        }

        boolean success;
        try {
            success = executor.invokeAll(tasks).stream().allMatch(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    logger.warn("Exception in delete task: {}", e.getMessage());
                    return false;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Delete operation interrupted");
            return false;
        }
        return success;
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void streamReconstruct(int partition, String storageKey,
            List<InetSocketAddress> nodes, OutputStream out)
            throws IOException {

        InputStream[] ins = new InputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        for (int i = 0; i < TOTAL; i++) {
            try {
                InetSocketAddress node = nodes.get(i);
                URI uri = URI.create(String.format("http://%s:%d/store/%d/%s",
                        node.getHostString(), node.getPort(), partition, storageKey));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .GET()
                        .build();

                HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

                if (response.statusCode() == 200) {
                    ins[i] = new ByteArrayInputStream(response.body());
                    present[i] = true;
                } else {
                    present[i] = false;
                }
            } catch (Exception e) {
                present[i] = false;
            }
        }

        int count = 0;
        for (boolean b : present)
            if (b)
                count++;
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

    private List<InetSocketAddress> nodesForKey(int setNum) {
        return switch (setNum) {
            case 1 -> set1;
            case 2 -> set2;
            default -> set3;
        };
    }
}