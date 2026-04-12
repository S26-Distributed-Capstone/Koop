package com.github.koop.queryprocessor.processor;

import com.github.koop.common.Util;
import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration.PartitionSpread;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
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
    private final CommitCoordinator commitCoordinator;
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
        this(set1, set2, set3, 0);
    }

    // Overload that accepts a pre-built CommitCoordinator — used in tests that
    // share a PubSubClient bus between the coordinator and the fake SNs.
    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2,
                         List<InetSocketAddress> set3, CommitCoordinator commitCoordinator) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        MemoryFetcher fetcher = new MemoryFetcher();
        this.metadataClient = new MetadataClient(fetcher);
        this.commitCoordinator = commitCoordinator;
        registerListeners();
        this.metadataClient.start();
        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        esConfig.setErasureSets(List.of(
                toErasureSet(1, set1),
                toErasureSet(2, set2),
                toErasureSet(3, set3)));
        fetcher.update(esConfig);
        fetcher.update(buildTestPartitionSpread());
    }

    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3, int ackPort) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        MemoryFetcher fetcher = new MemoryFetcher();
        this.metadataClient = new MetadataClient(fetcher);
        try {
            // MemoryPubSub routes messages back into the same process — sufficient
            // for unit tests; replace with a Kafka-backed PubSubClient in prod.
            PubSubClient pubSubClient = new PubSubClient(new MemoryPubSub());
            pubSubClient.start();
            this.commitCoordinator = new CommitCoordinator(pubSubClient, ackPort);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start CommitCoordinator ACK server", e);
        }
        registerListeners();
        this.metadataClient.start();
        ErasureSetConfiguration esConfig = new ErasureSetConfiguration();
        esConfig.setErasureSets(List.of(
                toErasureSet(1, set1),
                toErasureSet(2, set2),
                toErasureSet(3, set3)));
        fetcher.update(esConfig);
        fetcher.update(buildTestPartitionSpread());
    }

    // Convenience constructor that restores the original single-arg signature.
    // Creates a MemoryPubSub-backed CommitCoordinator on an OS-assigned port,
    // which is sufficient for tests that supply their own MetadataClient.
    public StorageWorker(MetadataClient metadataClient) {
        this(metadataClient, buildDefaultCommitCoordinator());
    }

    public StorageWorker(MetadataClient metadataClient, CommitCoordinator commitCoordinator) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.metadataClient = metadataClient;
        this.commitCoordinator = commitCoordinator;
        registerListeners();
        tryRebuildRouting();
    }

    private static CommitCoordinator buildDefaultCommitCoordinator() {
        try {
            PubSubClient pubSubClient = new PubSubClient(new MemoryPubSub());
            pubSubClient.start();
            return new CommitCoordinator(pubSubClient, 0);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start CommitCoordinator ACK server", e);
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Executes a full PUT:
     * <ol>
     *   <li>Erasure-code and stream shards to all {@value com.github.koop.common.erasure.ErasureCoder#TOTAL}
     *       storage nodes concurrently.</li>
     *   <li>Publish a commit message to the per-partition Kafka topic so every SN
     *       applies the operation to its op-log and metadata. SNs that missed the
     *       stream reconstruct their shard from peers before committing.</li>
     *   <li>Block until {@link CommitCoordinator#QUORUM} SN commit-ACKs are received,
     *       then return {@code true} to the caller.</li>
     * </ol>
     */
    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");
        if (data == null)      throw new IllegalArgumentException("data is null");
        if (length < 0)        throw new IllegalArgumentException("length < 0");

        String storageKey = bucket+"/"+key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<List<InetSocketAddress>> nodes = partition.isPresent()
                ? r.getNodes(partition.getAsInt())
                : Optional.empty();
        if (partition.isEmpty() || nodes.isEmpty()) {
            logger.error("Routing failed for key {}, aborting put", storageKey);
            return false;
        }
        int resolvedPartition = partition.getAsInt();
        List<InetSocketAddress> resolvedNodes = nodes.get();

        // Phase 1 – stream erasure-coded shards to all storage nodes concurrently.
        //
        // All shard streams MUST be drained concurrently: ErasureCoder.shard()
        // writes into 9 piped streams from a single encoder thread, so reading
        // them sequentially would deadlock.
        InputStream[] shardStreams = ErasureCoder.shard(data, length);

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            tasks.add(() -> {
                InetSocketAddress node = resolvedNodes.get(index);
                URI uri = URI.create(String.format(
                        "http://%s:%d/store/%d/%s?requestId=%s",
                        node.getHostString(), node.getPort(),
                        resolvedPartition, storageKey, requestID));
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(uri)
                            .PUT(HttpRequest.BodyPublishers.ofInputStream(() -> shardStreams[index]))
                            .header("Content-Type", "application/octet-stream")
                            .build();
                    HttpResponse<String> response =
                            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        logger.trace("PUT shard {} → node {} succeeded", index, node);
                        return true;
                    }
                    logger.trace("PUT shard {} → node {} HTTP {}", index, node, response.statusCode());
                } catch (Exception e) {
                    logger.trace("PUT shard {} → node {} exception: {}", index, node, e.getMessage());
                }
                return false;
            });
        }

        long uploaded;
        try {
            uploaded = executor.invokeAll(tasks).stream().filter(f -> {
                try { return f.get(); }
                catch (InterruptedException | ExecutionException e) {
                    logger.warn("Shard upload task error: {}", e.getMessage());
                    return false;
                }
            }).count();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        logger.debug("Phase 1 complete: {}/{} shards uploaded for requestId {}", uploaded, TOTAL, requestID);

        // Phase 2 – commit.
        //
        // Publish the commit message to the partition's Kafka topic. Every SN will:
        //   - add the op to its op-log + write metadata, if it received the shard; or
        //   - reconstruct the shard from peers and then commit, if it didn't.
        // SNs POST an ACK back to this QP's server. We block until QUORUM ACKs arrive.
        boolean committed = commitCoordinator.beginCommit(requestID, resolvedPartition, bucket, key);
        if (!committed) {
            logger.error("Commit phase failed for requestId {} (quorum ACKs not received)", requestID);
        }
        return committed;
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null)    throw new IllegalArgumentException("bucket is null");
        if (key == null)       throw new IllegalArgumentException("key is null");

        String storageKey = bucket+"/"+key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<List<InetSocketAddress>> nodes = partition.isPresent()
                ? r.getNodes(partition.getAsInt())
                : Optional.empty();
        if (partition.isEmpty() || nodes.isEmpty()) {
            logger.error("Routing failed for key {}, aborting get", storageKey);
            return InputStream.nullInputStream();
        }
        int resolvedPartition = partition.getAsInt();
        List<InetSocketAddress> resolvedNodes = nodes.get();

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(resolvedPartition, storageKey, resolvedNodes, pos);
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

        String storageKey = bucket+"/"+key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<List<InetSocketAddress>> nodes = partition.isPresent()
                ? r.getNodes(partition.getAsInt())
                : Optional.empty();
        if (partition.isEmpty() || nodes.isEmpty()) {
            logger.error("Routing failed for key {}, aborting delete", storageKey);
            return false;
        }
        int resolvedPartition = partition.getAsInt();
        List<InetSocketAddress> resolvedNodes = nodes.get();

        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            tasks.add(() -> {
                try {
                    InetSocketAddress node = resolvedNodes.get(index);
                    URI uri = URI.create(String.format("http://%s:%d/store/%d/%s",
                            node.getHostString(), node.getPort(), resolvedPartition, storageKey));

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
                    logger.warn("Exception in delete task for node {}: {}", resolvedNodes.get(index), e.getMessage());
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
        commitCoordinator.close();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void registerListeners() {
        this.metadataClient.listen(ErasureSetConfiguration.class, (prev, current) -> {
            logger.info("ErasureSetConfiguration updated: {} erasure sets",
                    current.getErasureSets() == null ? 0 : current.getErasureSets().size());
            tryRebuildRouting();
        });
        this.metadataClient.listen(PartitionSpreadConfiguration.class, (prev, current) -> {
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
        PartitionSpreadConfiguration ps = metadataClient.get(PartitionSpreadConfiguration.class);
        ErasureSetConfiguration es = metadataClient.get(ErasureSetConfiguration.class);
        if (ps != null && es != null) {
            routing.set(new ErasureRouting(ps, es));
            logger.info("ErasureRouting rebuilt");
        }else {
            logger.info("Cannot rebuild ErasureRouting yet (waiting for both configs): "
                    + "PartitionSpreadConfiguration is {}, ErasureSetConfiguration is {}",
                    ps == null ? "null" : "present", es == null ? "null" : "present");
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