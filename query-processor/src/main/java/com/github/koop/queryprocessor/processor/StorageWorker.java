package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.ReplicaSetConfiguration;
import com.github.koop.common.metadata.ReplicaSetConfiguration.Machine;
import com.github.koop.common.metadata.ReplicaSetConfiguration.ReplicaSet;

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
import java.util.concurrent.atomic.AtomicReference;

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

    private static final Logger logger = LogManager.getLogger(StorageWorker.class);

    private final ExecutorService executor;
    private final HttpClient httpClient;
    private final MetadataClient metadataClient;
    private final AtomicReference<ReplicaSetConfiguration> replicaSetConfig = new AtomicReference<>();

    // FOR TESTING ONLY - constructs a MetadataClient backed by a MemoryFetcher
    // with an empty configuration; use the three-arg constructor to supply nodes.
    public StorageWorker() {
        this(List.of(), List.of(), List.of());
    }

    // FOR TESTING ONLY - builds a MetadataClient backed by a MemoryFetcher and
    // pre-populates it with a ReplicaSetConfiguration derived from the three
    // address lists, assigning them set numbers 1, 2, and 3 respectively.
    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        MemoryFetcher fetcher = new MemoryFetcher();
        this.metadataClient = new MetadataClient(fetcher);
        this.metadataClient.listen(ReplicaSetConfiguration.class, (prev, current) -> {
            replicaSetConfig.set(current);
            logger.info("ReplicaSetConfiguration updated: {} replica sets",
                    current.getReplicaSets() == null ? 0 : current.getReplicaSets().size());
        });
        this.metadataClient.start();
        // update() must come after start() so the listener registered above is
        // already in place when MemoryFetcher fires it synchronously.
        ReplicaSetConfiguration config = new ReplicaSetConfiguration();
        config.setReplicaSets(List.of(
                toReplicaSet(1, set1),
                toReplicaSet(2, set2),
                toReplicaSet(3, set3)));
        fetcher.update(config);
    }

    public StorageWorker(MetadataClient metadataClient) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.metadataClient = metadataClient;
        this.metadataClient.listen(ReplicaSetConfiguration.class, (prev, current) -> {
            replicaSetConfig.set(current);
            logger.info("ReplicaSetConfiguration updated: {} replica sets",
                    current.getReplicaSets() == null ? 0 : current.getReplicaSets().size());
        });
        this.metadataClient.start();
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

        List<InetSocketAddress> nodes = nodesForSet(setNum);

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

        List<InetSocketAddress> nodes = nodesForSet(setNum);

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

        List<InetSocketAddress> nodes = nodesForSet(setNum);
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

    /**
     * Looks up the {@link InetSocketAddress} list for the given set number from
     * the live {@link ReplicaSetConfiguration} held by the metadata client.
     *
     * <p>The config is updated in-place via the {@link MetadataClient} listener
     * registered in the constructor, so callers always get the most recent view
     * without restarting the worker.
     *
     * @throws IllegalStateException if no configuration has been received yet,
     *                               or if {@code setNum} is not present in it.
     */
    private List<InetSocketAddress> nodesForSet(int setNum) {
        ReplicaSetConfiguration config = replicaSetConfig.get();
        if (config == null) {
            throw new IllegalStateException("ReplicaSetConfiguration has not been received from metadata yet");
        }
        return config.getReplicaSets().stream()
                .filter(rs -> rs.getNumber() == setNum)
                .findFirst()
                .map(rs -> rs.getMachines().stream()
                        .map(m -> new InetSocketAddress(m.getIp(), m.getPort()))
                        .toList())
                .orElseThrow(() -> new IllegalStateException("No replica set found for set number: " + setNum));
    }

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

    private static ReplicaSet toReplicaSet(int number, List<InetSocketAddress> addresses) {
        ReplicaSet rs = new ReplicaSet();
        rs.setNumber(number);
        rs.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return rs;
    }
}