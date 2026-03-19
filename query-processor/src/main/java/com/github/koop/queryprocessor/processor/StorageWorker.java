package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration.PartitionSpread;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
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
    private final AtomicReference<ErasureSetConfiguration> erasureSetConfig = new AtomicReference<>();
    private final AtomicReference<PartitionSpreadConfiguration> partitionSpreadConfig = new AtomicReference<>();
    private final AtomicReference<ErasureRouting> routing = new AtomicReference<>();

    // FOR TESTING ONLY - constructs a MetadataClient backed by a MemoryFetcher
    // with an empty configuration; use the three-arg constructor to supply nodes.
    public StorageWorker() {
        this(List.of(), List.of(), List.of());
    }

    // FOR TESTING ONLY - builds a MetadataClient backed by a MemoryFetcher and
    // pre-populates it with an ErasureSetConfiguration derived from the three
    // address lists (set numbers 1, 2, 3) and a matching PartitionSpreadConfiguration
    // with 99 partitions spread evenly across the three sets (33 each).
    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        MemoryFetcher fetcher = new MemoryFetcher();
        this.metadataClient = new MetadataClient(fetcher);
        registerListeners();
        this.metadataClient.start();
        // update() must come after start() so the listeners registered above are
        // already in place when MemoryFetcher fires them synchronously.
        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        esConfig.setErasureSets(List.of(
                toErasureSet(1, set1),
                toErasureSet(2, set2),
                toErasureSet(3, set3)));
        fetcher.update(esConfig);
        fetcher.update(buildTestPartitionSpread());
    }

    public StorageWorker(MetadataClient metadataClient) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.metadataClient = metadataClient;
        registerListeners();
        this.metadataClient.start();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");
        if (data == null)      throw new IllegalArgumentException("data is null");
        if (length < 0)        throw new IllegalArgumentException("length < 0");

        String storageKey = bucket + "-" + key;
        ErasureRouting r = getRouting();
        int partition = r.getPartition(storageKey);
        List<InetSocketAddress> nodes = r.getNodes(partition);
        if (partition == ErasureRouting.INVALID_PARTITION || nodes == ErasureRouting.INVALID_NODES) {
            logger.error("Routing failed for key {}, aborting put", storageKey);
            return false;
        }

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
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        ErasureRouting r = getRouting();
        int partition = r.getPartition(storageKey);
        List<InetSocketAddress> nodes = r.getNodes(partition);
        if (partition == ErasureRouting.INVALID_PARTITION || nodes == ErasureRouting.INVALID_NODES) {
            logger.error("Routing failed for key {}, aborting get", storageKey);
            return InputStream.nullInputStream();
        }

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, nodes, pos);
            } catch (Exception e) {
                try { pos.close(); } catch (IOException ignored) {}
            }
        });

        return pis;
    }

    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        ErasureRouting r = getRouting();
        int partition = r.getPartition(storageKey);
        List<InetSocketAddress> nodes = r.getNodes(partition);
        if (partition == ErasureRouting.INVALID_PARTITION || nodes == ErasureRouting.INVALID_NODES) {
            logger.error("Routing failed for key {}, aborting delete", storageKey);
            return false;
        }

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

    private void registerListeners() {
        this.metadataClient.listen(ErasureSetConfiguration.class, (prev, current) -> {
            erasureSetConfig.set(current);
            logger.info("ErasureSetConfiguration updated: {} erasure sets",
                    current.getErasureSets() == null ? 0 : current.getErasureSets().size());
            tryRebuildRouting();
        });
        this.metadataClient.listen(PartitionSpreadConfiguration.class, (prev, current) -> {
            partitionSpreadConfig.set(current);
            logger.info("PartitionSpreadConfiguration updated: {} spread entries",
                    current.getPartitionSpread() == null ? 0 : current.getPartitionSpread().size());
            tryRebuildRouting();
        });
    }

    /**
     * Rebuilds the shared {@link ErasureRouting} instance whenever either config
     * is updated. Both configs must be present before routing can be constructed;
     * if one hasn't arrived yet this is a no-op and the first update of the
     * lagging config will trigger the rebuild instead.
     */
    private void tryRebuildRouting() {
        PartitionSpreadConfiguration ps = partitionSpreadConfig.get();
        ErasureSetConfiguration es = erasureSetConfig.get();
        if (ps != null && es != null) {
            routing.set(new ErasureRouting(ps, es));
            logger.info("ErasureRouting rebuilt");
        }
    }

    /**
     * Returns the current {@link ErasureRouting}, which is rebuilt automatically
     * whenever either config is updated via the metadata listener.
     */
    private ErasureRouting getRouting() {
        ErasureRouting r = routing.get();
        if (r == null)
            throw new IllegalStateException(
                    "ErasureRouting is not ready — waiting for PartitionSpreadConfiguration and ErasureSetConfiguration");
        return r;
    }

    private void streamReconstruct(int partition, String storageKey,
                                   List<InetSocketAddress> nodes, OutputStream out) throws IOException {

        InputStream[] ins = new InputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        for (int i = 0; i < TOTAL; i++) {
            try {
                InetSocketAddress node = nodes.get(i);
                URI uri = URI.create(String.format("http://%s:%d/store/%d/%s",
                        node.getHostString(), node.getPort(), partition, storageKey));

                HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();
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
        for (boolean b : present) if (b) count++;
        if (count < ErasureCoder.K)
            throw new IOException("lost too many shards; need " + ErasureCoder.K + ", got " + count);

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present)) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = reconstructed.read(buf)) != -1) out.write(buf, 0, n);
        }
        out.flush();
    }

    /**
     * FOR TESTING ONLY - builds a PartitionSpreadConfiguration with 99 partitions
     * spread evenly: partitions 0-32 → erasure set 1, 33-65 → erasure set 2, 66-98 → erasure set 3.
     */
    private static PartitionSpreadConfiguration buildTestPartitionSpread() {
        PartitionSpreadConfiguration ps = new PartitionSpreadConfiguration();
        List<PartitionSpread> spreads = new ArrayList<>();
        for (int s = 0; s < 3; s++) {
            PartitionSpread spread = new PartitionSpread();
            spread.setErasureSet(s + 1); // set numbers 1, 2, 3
            List<Integer> partitions = new ArrayList<>();
            for (int p = s * 33; p < (s + 1) * 33; p++) partitions.add(p);
            spread.setPartitions(partitions);
            spreads.add(spread);
        }
        ps.setPartitionSpread(spreads);
        return ps;
    }

    private static ErasureSet toErasureSet(int number, List<InetSocketAddress> addresses) {
        ErasureSet es = new ErasureSet();
        es.setNumber(number);
        es.setMachines(addresses.stream().map(addr -> {
            Machine m = new Machine();
            m.setIp(addr.getHostString());
            m.setPort(addr.getPort());
            return m;
        }).toList());
        return es;
    }
}