package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.Message;
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

        String storageKey = bucket + "/" + key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<ErasureSetConfiguration.ErasureSet> erasureSetOpt = partition.isPresent()
                ? r.getErasureSet(partition.getAsInt())
                : Optional.empty();

        if (partition.isEmpty() || erasureSetOpt.isEmpty()) {
            logger.error("Routing failed for key {}, aborting put", storageKey);
            return false;
        }

        int resolvedPartition = partition.getAsInt();
        ErasureSetConfiguration.ErasureSet es = erasureSetOpt.get();
        List<InetSocketAddress> resolvedNodes = es.getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        int n = es.getN();
        int k = es.getK();
        int writeQuorum = es.getWriteQuorum();

        // Phase 1 – stream erasure-coded shards to all storage nodes concurrently.
        InputStream[] shardStreams = ErasureCoder.shard(data, length, k, n);

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
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
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
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
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Shard upload task error: {}", e.getMessage());
                    return false;
                }
            }).count();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (uploaded < writeQuorum) {
            logger.error("Phase 1 failed: only {}/{} shards uploaded. Aborting commit.", uploaded, n);
            return false;
        }

        logger.debug("Phase 1 complete: {}/{} shards uploaded for requestId {}", uploaded, n, requestID);

        // Phase 2 – commit.
        boolean committed = commitCoordinator.beginCommit(requestID, resolvedPartition, bucket, key, writeQuorum);
        if (!committed) {
            logger.error("Commit phase failed for requestId {} (quorum ACKs not received)", requestID);
        }
        return committed;
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "/" + key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<ErasureSetConfiguration.ErasureSet> erasureSetOpt = partition.isPresent()
                ? r.getErasureSet(partition.getAsInt())
                : Optional.empty();

        if (partition.isEmpty() || erasureSetOpt.isEmpty()) {
            logger.error("Routing failed for key {}, aborting get", storageKey);
            return InputStream.nullInputStream();
        }

        int resolvedPartition = partition.getAsInt();
        ErasureSetConfiguration.ErasureSet es = erasureSetOpt.get();
        List<InetSocketAddress> resolvedNodes = es.getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(resolvedPartition, storageKey, resolvedNodes, es.getK(), es.getN(), pos);
            } catch (Exception e) {
                logger.error("Failed to reconstruct stream for key {}", storageKey, e);
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

        String storageKey = bucket + "/" + key;
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        Optional<ErasureSetConfiguration.ErasureSet> erasureSetOpt = partition.isPresent()
                ? r.getErasureSet(partition.getAsInt())
                : Optional.empty();

        if (partition.isEmpty() || erasureSetOpt.isEmpty()) {
            logger.error("Routing failed for key {}, aborting delete", storageKey);
            return false;
        }

        int resolvedPartition = partition.getAsInt();
        ErasureSetConfiguration.ErasureSet es = erasureSetOpt.get();
        List<InetSocketAddress> resolvedNodes = es.getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < es.getN(); i++) {
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

    public boolean beginMultipartCommit(String bucket, String key, String uploadId, List<String> chunks) {
        if (bucket == null)   throw new IllegalArgumentException("bucket is null");
        if (key == null)      throw new IllegalArgumentException("key is null");
        if (uploadId == null) throw new IllegalArgumentException("uploadId is null");
        if (chunks == null)   throw new IllegalArgumentException("chunks is null");

        UUID requestId;
        try {
            requestId = UUID.fromString(uploadId);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid multipart uploadId {}, cannot begin commit", uploadId);
            return false;
        }

        String storageKey = toStorageKey(bucket, key);
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(storageKey);
        if (partition.isEmpty()) {
            logger.error("Routing failed for key {}, aborting multipart commit", storageKey);
            return false;
        }

        Optional<ErasureSetConfiguration.ErasureSet> erasureSetOpt = r.getErasureSet(partition.getAsInt());
        if (erasureSetOpt.isEmpty()) {
            logger.error("Routing failed for key {}, aborting multipart commit", storageKey);
            return false;
        }

        return commitCoordinator.beginMultipartCommit(requestId, partition.getAsInt(), bucket, key, chunks, erasureSetOpt.get().getWriteQuorum());
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

    private void tryRebuildRouting() {
        PartitionSpreadConfiguration ps = metadataClient.get(PartitionSpreadConfiguration.class);
        ErasureSetConfiguration es = metadataClient.get(ErasureSetConfiguration.class);
        if (ps != null && es != null) {
            routing.set(new ErasureRouting(ps, es));
            logger.info("ErasureRouting rebuilt");
        } else {
            logger.info("Cannot rebuild ErasureRouting yet (waiting for both configs): "
                    + "PartitionSpreadConfiguration is {}, ErasureSetConfiguration is {}",
                    ps == null ? "null" : "present", es == null ? "null" : "present");
        }
    }

    private ErasureRouting getRouting() {
        ErasureRouting r = routing.get();
        if (r == null)
            throw new IllegalStateException(
                    "ErasureRouting is not ready — waiting for PartitionSpreadConfiguration and ErasureSetConfiguration");
        return r;
    }

    private static String toStorageKey(String bucket, String key) {
        return bucket + "/" + key;
    }

    private void streamReconstruct(int partition, String storageKey,
            List<InetSocketAddress> nodes, int k, int n, OutputStream out) throws IOException {

        InputStream[] ins = new InputStream[n];
        boolean[] present = new boolean[n];

        for (int i = 0; i < n; i++) {
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
                logger.warn("Failed to fetch shard {} from node {}", i, nodes.get(i), e);
                present[i] = false;
            }
        }

        int count = 0;
        for (boolean b : present)
            if (b)
                count++;
        if (count < k)
            throw new IOException("lost too many shards; need " + k + ", got " + count);

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present, k, n)) {
            byte[] buf = new byte[64 * 1024];
            int bytesRead;
            while ((bytesRead = reconstructed.read(buf)) != -1)
                out.write(buf, 0, bytesRead);
        }
        out.flush();
    }

    private static PartitionSpreadConfiguration buildTestPartitionSpread() {
        PartitionSpreadConfiguration ps = new PartitionSpreadConfiguration();
        List<PartitionSpread> spreads = new ArrayList<>();
        for (int s = 0; s < 3; s++) {
            PartitionSpread spread = new PartitionSpread();
            spread.setErasureSet(s + 1); // set numbers 1, 2, 3
            List<Integer> partitions = new ArrayList<>();
            for (int p = s * 33; p < (s + 1) * 33; p++)
                partitions.add(p);
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