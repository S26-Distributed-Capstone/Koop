package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.ErasureRouting;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.google.protobuf.ByteString.Output;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    /**
     * Polling interval (ms) for the health watcher that cancels in-flight transfers
     * to DOWN nodes.
     */
    private static final long HEALTH_WATCH_INTERVAL_MS = 250;

    private final ExecutorService executor;
    private final HttpClient httpClient;
    private final MetadataClient metadataClient;
    private final CommitCoordinator commitCoordinator;
    private final NodeHealthTracker healthTracker;
    private final AtomicReference<ErasureRouting> routing = new AtomicReference<>();

    // Wrapper record to return both the stream and its exact byte size
    public record RetrievedObject(InputStream stream, long size) {
    }

    // Convenience constructor — creates a MemoryPubSub-backed CommitCoordinator
    // on an OS-assigned port. Useful when only a MetadataClient is available.
    public StorageWorker(MetadataClient metadataClient) {
        this(metadataClient, buildDefaultCommitCoordinator(), new NodeHealthTracker());
    }

    public StorageWorker(MetadataClient metadataClient, CommitCoordinator commitCoordinator) {
        this(metadataClient, commitCoordinator, new NodeHealthTracker());
    }

    public StorageWorker(MetadataClient metadataClient,
            CommitCoordinator commitCoordinator,
            NodeHealthTracker healthTracker) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.metadataClient = metadataClient;
        this.commitCoordinator = commitCoordinator;
        this.healthTracker = healthTracker != null ? healthTracker : new NodeHealthTracker();
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
        int m = es.getM();
        int writeQuorum = es.getWriteQuorum();

        // DOWN nodes are skipped unconditionally. If the healthy set can't meet
        // write quorum, fail fast — no point starting the erasure-coding producer.
        Set<InetSocketAddress> healthyForWrite = healthTracker.getHealthyNodes(resolvedNodes);
        if (healthyForWrite.size() < writeQuorum) {
            logger.error("Aborting put: only {} healthy nodes available, writeQuorum={}",
                    healthyForWrite.size(), writeQuorum);
            return false;
        }

        // Phase 1 – stream erasure-coded shards to all storage nodes concurrently.
        InputStream[] shardStreams = ErasureCoder.shard(data, length, m, n);

        List<InflightTransfer> inflight = new ArrayList<>();
        List<CompletableFuture<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int index = i;
            InetSocketAddress node = resolvedNodes.get(index);
            if (!healthyForWrite.contains(node)) {
                logger.trace("PUT shard {} → node {} skipped (marked DOWN)", index, node);
                // Drain-and-discard the unread shard stream. ErasureCoder.shard() feeds
                // every pipe regardless of consumer; if we leave this one unread, its
                // bounded queue fills up and the producer stalls (~10s) on enqueue,
                // starving the healthy pipes too. Closing instead is racy: the producer's
                // prefix-write loop has no per-pipe try/catch, so a close that lands
                // before pos[i].write(lenBytes) triggers fail() on every pipe.
                executor.execute(() -> drainAndClose(shardStreams[index]));
                results.add(CompletableFuture.completedFuture(false));
                continue;
            }
            URI uri;
            try {
                uri = new URI("http", null, node.getHostString(), node.getPort(),
                        "/store/" + resolvedPartition + "/" + storageKey,
                        "requestId=" + requestID, null);
            } catch (URISyntaxException e) {
                logger.error("Failed to build URI for node {}: {}", node, e.getMessage());
                return false;
            }
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .PUT(HttpRequest.BodyPublishers.ofInputStream(() -> shardStreams[index]))
                    .header("Content-Type", "application/octet-stream")
                    .build();

            CompletableFuture<HttpResponse<String>> raw = httpClient.sendAsync(request,
                    HttpResponse.BodyHandlers.ofString());
            CompletableFuture<Boolean> result = raw.handle((response, ex) -> {
                if (ex != null) {
                    Throwable cause = unwrap(ex);
                    if (cause instanceof CancellationException) {
                        logger.trace("PUT shard {} → node {} cancelled (health watcher)", index, node);
                    } else {
                        healthTracker.recordFailure(node);
                        logger.trace("PUT shard {} → node {} exception: {}", index, node, cause.getMessage());
                    }
                    return false;
                }
                if (response.statusCode() == 200) {
                    healthTracker.recordSuccess(node);
                    logger.trace("PUT shard {} → node {} succeeded", index, node);
                    return true;
                }
                healthTracker.recordFailure(node);
                logger.trace("PUT shard {} → node {} HTTP {}", index, node, response.statusCode());
                return false;
            });

            inflight.add(new InflightTransfer(node, raw, index));
            results.add(result);
        }

        startHealthWatcher(inflight);

        long uploaded = 0;
        for (CompletableFuture<Boolean> f : results) {
            try {
                if (Boolean.TRUE.equals(f.get())) {
                    uploaded++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (CancellationException | ExecutionException e) {
                logger.trace("Shard upload task error: {}", e.getMessage());
            }
        }

        if (uploaded < writeQuorum) {
            logger.error("Phase 1 failed: only {}/{} shards uploaded. Aborting commit.", uploaded, n);
            return false;
        }

        logger.debug("Phase 1 complete: {}/{} shards uploaded for requestId {}", uploaded, n, requestID);

        // Phase 2 – commit. (Pass the exact length to the database metadata)
        boolean committed = commitCoordinator.beginCommit(requestID, resolvedPartition, bucket, key, length,
                writeQuorum);
        if (!committed) {
            logger.error("Commit phase failed for requestId {} (quorum ACKs not received)", requestID);
        }
        return committed;
    }

    public RetrievedObject get(UUID requestID, String bucket, String key) throws IOException {
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
            return null;
        }

        int resolvedPartition = partition.getAsInt();
        ErasureSetConfiguration.ErasureSet es = erasureSetOpt.get();
        var resolvedNodes = es.getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        int m = es.getM();
        int n = es.getN();

        // Synchronous probe: fetch shard metadata to detect tombstones/type
        // BEFORE creating the piped stream. This lets the caller (gateway)
        // distinguish "deleted" (404) from real errors (500).
        List<ShardResponse> responses = fetchShards(resolvedPartition, storageKey, resolvedNodes, OptionalLong.empty());

        if (responses.isEmpty()) {
            logger.debug("No shards found for key {} — object does not exist", storageKey);
            return null;
        }

        long maxVersion = responses.stream().mapToLong(ShardResponse::version).max().orElse(-1);
        List<ShardResponse> maxVersionResponses = responses.stream().filter(sr -> sr.version() == maxVersion).toList();
        ShardResponse representative = maxVersionResponses.get(0);

        if (representative.type() == ShardType.TOMBSTONE) {
            logger.debug("Latest version {} for key {} is a tombstone.", maxVersion, storageKey);
            return null;
        }

        // Object exists — set up piped stream and reconstruct asynchronously
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        if (representative.type() == ShardType.MULTIPART) {
            logger.debug("Latest version {} for key {} is multipart. Streaming chunks sequentially.", maxVersion,
                    storageKey);
            executor.execute(() -> {
                try (pos) {
                    handleMultipartGet(representative.chunks(), m, pos);
                } catch (Exception e) {
                    logger.error("Failed to stream multipart data for key {}", storageKey, e);
                }
            });
        } else {
            // BLOB — reconstruct from erasure-coded shards
            executor.execute(() -> {
                try (pos) {
                    if (maxVersionResponses.size() >= m) {
                        reconstructFromResponses(maxVersionResponses, resolvedNodes, m, n, pos);
                    } else {
                        reconstructFromOlderVersion(resolvedPartition, storageKey, resolvedNodes, m, n, pos,
                                maxVersion, maxVersionResponses.size(), responses);
                    }
                } catch (Exception e) {
                    logger.error("Failed to reconstruct stream for key {}", storageKey, e);
                }
            });
        }

        // Return both the stream and the logical size extracted from the storage node
        // headers
        return new RetrievedObject(pis, representative.logicalSize());
    }

    private void processGetConsensus(int partition, String storageKey, List<InetSocketAddress> nodes, int m, int n,
            OutputStream out) throws IOException {
        // Initial fetch: gets data + metadata in one pass without specifying a version
        List<ShardResponse> responses = fetchShards(partition, storageKey, nodes, OptionalLong.empty());

        if (responses.isEmpty()) {
            throw new IOException("No reachable nodes for key " + storageKey);
        }

        // Determine highest version reported
        long maxVersion = responses.stream().mapToLong(ShardResponse::version).max().orElse(-1);
        List<ShardResponse> maxVersionResponses = responses.stream().filter(r -> r.version() == maxVersion).toList();
        ShardResponse representative = maxVersionResponses.get(0);

        if (representative.type() == ShardType.TOMBSTONE) {
            // Reachable only via handleMultipartGet — a multipart chunk should never be
            // independently tombstoned. The top-level get() handles tombstones at the
            // object level and returns null before reaching here.
            logger.error("Multipart chunk {} (version {}) is tombstoned — multipart object is corrupted.",
                    storageKey, maxVersion);
            throw new IOException("Multipart chunk " + storageKey + " is missing (tombstoned)");
        }

        if (representative.type() == ShardType.MULTIPART) {
            logger.debug("Latest version {} for key {} is multipart. Streaming chunks sequentially.", maxVersion,
                    storageKey);
            handleMultipartGet(representative.chunks(), m, out);
            return;
        }

        // Type is BLOB
        if (maxVersionResponses.size() >= m) {
            reconstructFromResponses(maxVersionResponses, nodes, m, n, out);
            return;
        }

        reconstructFromOlderVersion(partition, storageKey, nodes, m, n, out, maxVersion, maxVersionResponses.size(),
                responses);
    }

    private void reconstructFromOlderVersion(int partition, String storageKey, List<InetSocketAddress> nodes, int m,
            int n,
            OutputStream out, long maxVersion, int maxVersionShardCount, List<ShardResponse> responses)
            throws IOException {
        logger.warn("Latest version {} for key {} has insufficient shards ({}/{}). Searching older versions.",
                maxVersion, storageKey, maxVersionShardCount, m);

        List<Long> availableVersions = responses.stream()
                .map(ShardResponse::version)
                .distinct()
                .sorted(Comparator.reverseOrder())
                .toList();

        if (availableVersions.size() < 2) {
            throw new IOException("Only one version available for key " + storageKey
                    + " and it has insufficient shards (" + maxVersionShardCount + "/" + m + ")");
        }

        long oldVersion = availableVersions.get(1); // second-highest version
        List<ShardResponse> oldResponses = fetchShards(partition, storageKey, nodes, OptionalLong.of(oldVersion));
        List<ShardResponse> validOldResponses = oldResponses.stream().filter(r -> r.version() == oldVersion).toList();
        if (validOldResponses.size() >= m) {
            logger.debug("Found sufficient shards for version {} ({}/{}). Reconstructing.", oldVersion,
                    validOldResponses.size(), m);
            reconstructFromResponses(validOldResponses, nodes, m, n, out);
            return;
        }

        throw new IOException("Could not find any version with sufficient shards for key " + storageKey);
    }

    private void handleMultipartGet(List<String> chunks, int m, OutputStream out) throws IOException {
        for (String chunkId : chunks) {
            var partition = getRouting().getPartition(chunkId)
                    .orElseThrow(() -> new IOException("No partition for chunk " + chunkId));
            List<InetSocketAddress> nodes = getNodesForPartition(partition);
            processGetConsensus(partition, chunkId, nodes, m, nodes.size(), out);
        }
    }

    private List<ShardResponse> fetchShards(int partition, String storageKey, List<InetSocketAddress> nodes,
            OptionalLong targetVersion) {
        // DOWN nodes are skipped unconditionally. If too few shards come back, the
        // caller surfaces the failure to the client — no fallback to DOWN nodes.
        Set<InetSocketAddress> healthyForRead = healthTracker.getHealthyNodes(nodes);

        List<InflightTransfer> inflight = new ArrayList<>();
        List<CompletableFuture<ShardResponse>> results = new ArrayList<>();

        for (int i = 0; i < nodes.size(); i++) {
            final int index = i;
            InetSocketAddress node = nodes.get(index);
            if (!healthyForRead.contains(node)) {
                logger.trace("GET shard {} → node {} skipped (marked DOWN)", index, node);
                results.add(CompletableFuture.completedFuture(null));
                continue;
            }
            String query = targetVersion.isPresent() ? "version=" + targetVersion.getAsLong() : null;
            URI uri;
            try {
                uri = new URI("http", null, node.getHostString(), node.getPort(),
                        "/store/" + partition + "/" + storageKey,
                        query, null);
            } catch (URISyntaxException e) {
                logger.error("Failed to build URI for node {}: {}", node, e.getMessage());
                return List.of();
            }

            HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();

            CompletableFuture<HttpResponse<InputStream>> raw = httpClient.sendAsync(request,
                    HttpResponse.BodyHandlers.ofInputStream());
            CompletableFuture<ShardResponse> result = raw.handle((response, ex) -> {
                if (ex != null) {
                    Throwable cause = unwrap(ex);
                    if (cause instanceof CancellationException) {
                        logger.trace("GET shard {} → node {} cancelled (health watcher)", index, node);
                    } else {
                        healthTracker.recordFailure(node);
                        logger.trace("GET shard {} → node {} exception: {}", index, node, cause.getMessage());
                    }
                    return null;
                }

                if (response.statusCode() == 200) {
                    healthTracker.recordSuccess(node);
                    long version = response.headers().firstValueAsLong("X-Koop-Version").orElse(-1L);
                    ShardType type = ShardType.fromHeader(response.headers().firstValue("X-Koop-Type").orElse("BLOB"));
                    long logicalSize = response.headers().firstValueAsLong("X-Koop-Size").orElse(-1L);

                    List<String> chunks = new ArrayList<>();
                    if (type == ShardType.MULTIPART) {
                        try {
                            byte[] payload = response.body().readAllBytes();
                            String json = new String(payload, java.nio.charset.StandardCharsets.UTF_8).trim();
                            // Parse simple JSON array: ["chunk1","chunk2"]
                            if (json.startsWith("[") && json.endsWith("]")) {
                                String content = json.substring(1, json.length() - 1);
                                if (!content.isEmpty()) {
                                    for (String token : content.split(",")) {
                                        chunks.add(token.trim().replace("\"", ""));
                                    }
                                }
                            }
                        } catch (IOException ioe) {
                            healthTracker.recordFailure(node);
                            logger.trace("GET shard {} → node {} multipart payload read failed: {}",
                                    index, node, ioe.getMessage());
                            return null;
                        }
                        return new ShardResponse(index, version, type, logicalSize, chunks,
                                InputStream.nullInputStream());
                    }

                    return new ShardResponse(index, version, type, logicalSize, List.of(), response.body());
                }
                if (response.statusCode() >= 500) {
                    healthTracker.recordFailure(node);
                }
                // 4xx (e.g. 404 missing shard) is not a health signal — leave tracker
                // untouched.
                return null;
            });

            inflight.add(new InflightTransfer(node, raw, index));
            results.add(result);
        }

        startHealthWatcher(inflight);

        List<ShardResponse> out = new ArrayList<>();
        for (CompletableFuture<ShardResponse> f : results) {
            try {
                ShardResponse sr = f.get();
                if (sr != null)
                    out.add(sr);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            } catch (CancellationException | ExecutionException e) {
                logger.trace("Shard fetch task error: {}", e.getMessage());
            }
        }
        return out;
    }

    private void reconstructFromResponses(List<ShardResponse> payloadResponses, List<InetSocketAddress> nodes, int m,
            int n, OutputStream out) throws IOException {
        InputStream[] ins = new InputStream[n];
        boolean[] present = new boolean[n];

        for (ShardResponse res : payloadResponses) {
            if (res.nodeIndex() < n) {
                ins[res.nodeIndex()] = res.payload();
                present[res.nodeIndex()] = true;
            }
        }

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present, m, n)) {
            byte[] buf = new byte[64 * 1024];
            int bytesRead;
            while ((bytesRead = reconstructed.read(buf)) != -1) {
                out.write(buf, 0, bytesRead);
            }
            out.flush();
        }
    }

    private enum ShardType {
        BLOB,
        MULTIPART,
        TOMBSTONE;

        static ShardType fromHeader(String value) {
            if (value == null) {
                return BLOB;
            }
            try {
                return ShardType.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return BLOB;
            }
        }
    }

    private record ShardResponse(int nodeIndex, long version, ShardType type, long logicalSize, List<String> chunks,
            InputStream payload) {
    }

    /**
     * Publishes a delete command via pub/sub and waits for {@code k + 1} SNs
     * (where k = parity shards = n - m) to acknowledge the tombstone.
     */
    public boolean delete(UUID requestID, String bucket, String key) {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = toStorageKey(bucket, key);
        Optional<PartitionAndSet> resolved = resolveErasureSet(storageKey);
        if (resolved.isEmpty()) {
            logger.error("Routing failed for key {}, aborting delete", storageKey);
            return false;
        }

        int deleteQuorum = resolved.get().erasureSet().getK() + 1;
        boolean deleted = commitCoordinator.beginDelete(
                requestID, resolved.get().partition(), bucket, key, deleteQuorum);
        if (!deleted) {
            logger.error("Delete commit failed for requestId {} (quorum ACKs not received)", requestID);
        }
        return deleted;
    }

    /**
     * Creates a bucket by hashing the bucket name to a partition, then publishing
     * a {@link Message.CreateBucketMessage} to that partition's Kafka topic and
     * waiting for {@code k + 1} SNs (the delete quorum) to acknowledge.
     *
     * @return {@code true} iff the required quorum of SNs ACKed within the timeout.
     */
    public boolean createBucket(UUID requestID, String bucket) {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");

        Optional<PartitionAndSet> resolved = resolveErasureSet(bucket);
        if (resolved.isEmpty()) {
            logger.error("Routing failed for bucket {}, aborting createBucket", bucket);
            return false;
        }

        int deleteQuorum = resolved.get().erasureSet().getK() + 1;
        boolean created = commitCoordinator.beginCreateBucket(
                requestID, resolved.get().partition(), bucket, deleteQuorum);
        if (!created) {
            logger.error("CreateBucket commit failed for requestId {} bucket {} (quorum ACKs not received)",
                    requestID, bucket);
        }
        return created;
    }

    /**
     * Deletes a bucket by hashing the bucket name to a partition, then publishing
     * a {@link Message.DeleteBucketMessage} to that partition's Kafka topic and
     * waiting for {@code k + 1} SNs (the delete quorum) to acknowledge.
     *
     * <p>
     * The bucket record is logically deleted (tombstoned in RocksDB). Objects
     * inside the bucket are not immediately purged; they are cleaned up
     * asynchronously by background compaction on each SN.
     *
     * @return {@code true} iff the required quorum of SNs ACKed within the timeout.
     */
    public boolean deleteBucket(UUID requestID, String bucket) {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");

        Optional<PartitionAndSet> resolved = resolveErasureSet(bucket);
        if (resolved.isEmpty()) {
            logger.error("Routing failed for bucket {}, aborting deleteBucket", bucket);
            return false;
        }

        int deleteQuorum = resolved.get().erasureSet().getK() + 1;
        boolean deleted = commitCoordinator.beginDeleteBucket(
                requestID, resolved.get().partition(), bucket, deleteQuorum);
        if (!deleted) {
            logger.error("DeleteBucket commit failed for requestId {} bucket {} (quorum ACKs not received)",
                    requestID, bucket);
        }
        return deleted;
    }

    /**
     * Checks whether a bucket exists by querying all nodes in the partition's
     * erasure set concurrently. Returns {@code true} if any node returns 200
     * (bucket exists), {@code false} only if every node returns 404 or fails.
     *
     * <p>
     * This prevents false negatives when a node is stale or partitioned:
     * even a single node reporting the bucket exists is sufficient.
     */
    public boolean bucketExists(String bucket) {
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");

        Optional<PartitionAndSet> resolved = resolveErasureSet(bucket);
        if (resolved.isEmpty()) {
            logger.error("Routing failed for bucket {}, aborting bucketExists", bucket);
            return false;
        }

        List<InetSocketAddress> nodes = resolved.get().erasureSet().getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        // DOWN nodes are skipped unconditionally. If every node is DOWN, return false.
        Set<InetSocketAddress> healthyForHead = healthTracker.getHealthyNodes(nodes);

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (InetSocketAddress node : nodes) {
            tasks.add(() -> {
                if (!healthyForHead.contains(node)) {
                    logger.trace("HEAD bucket {} → node {} skipped (marked DOWN)", bucket, node);
                    return false;
                }
                URI uri = URI.create(String.format("http://%s:%d/bucket/%s",
                        node.getHostString(), node.getPort(),
                        URLEncoder.encode(bucket, java.nio.charset.StandardCharsets.UTF_8)));
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(uri)
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .build();
                try {
                    HttpResponse<Void> resp = httpClient.send(req, HttpResponse.BodyHandlers.discarding());
                    int status = resp.statusCode();
                    // Any HTTP response means the node is reachable.
                    healthTracker.recordSuccess(node);
                    if (status == 200)
                        return true;
                    if (status == 404) {
                        logger.trace("HEAD bucket {} → node {} HTTP 404, trying next", bucket, node);
                        return false;
                    }
                    logger.trace("HEAD bucket {} → node {} HTTP {}, trying next", bucket, node, status);
                } catch (Exception e) {
                    healthTracker.recordFailure(node);
                    logger.trace("HEAD bucket {} → node {} failed: {}", bucket, node, e.getMessage());
                }
                return false;
            });
        }

        try {
            // Return true if ANY node reports the bucket exists
            return executor.invokeAll(tasks).stream().anyMatch(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.trace("bucketExists task error: {}", e.getMessage());
                    return false;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Lists objects in a bucket by fanning out a GET to one node per partition,
     * merging results, deduping by key, sorting, and applying maxKeys.
     */
    public List<ObjectInfo> listObjects(String bucket, String prefix, int maxKeys) {
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (maxKeys < 0)
            throw new IllegalArgumentException("maxKeys < 0");

        ErasureRouting r = getRouting();
        var spreads = metadataClient.get(PartitionSpreadConfiguration.class).getPartitionSpread();
        if (spreads == null || spreads.isEmpty()) {
            logger.error("listObjects: no partition spread");
            return List.of();
        }

        List<Integer> partitions = spreads.stream()
                .flatMap(s -> s.getPartitions().stream())
                .distinct()
                .toList();

        String safePrefix = prefix == null ? "" : prefix;

        // Encode the prefix and manually construct the query string
        String encodedPrefix = URLEncoder.encode(safePrefix, java.nio.charset.StandardCharsets.UTF_8);
        String query = "?maxKeys=" + maxKeys + (safePrefix.isEmpty() ? "" : "&prefix=" + encodedPrefix);

        List<Callable<List<ObjectInfo>>> tasks = new ArrayList<>();
        for (int partition : partitions) {
            Optional<ErasureSetConfiguration.ErasureSet> esOpt = r.getErasureSet(partition);
            if (esOpt.isEmpty())
                continue;
            List<InetSocketAddress> nodes = esOpt.get().getMachines().stream()
                    .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();
            if (nodes.isEmpty())
                continue;
            tasks.add(() -> fetchListFromAnyNode(bucket, query, nodes));
        }

        List<ObjectInfo> merged = new ArrayList<>();
        try {
            for (var future : executor.invokeAll(tasks)) {
                try {
                    merged.addAll(future.get());
                } catch (ExecutionException e) {
                    logger.warn("listObjects partition fetch failed: {}", e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }

        return merged.stream()
                .collect(java.util.stream.Collectors.toMap(ObjectInfo::key, o -> o, (a, b) -> a))
                .values().stream()
                .sorted(Comparator.comparing(ObjectInfo::key))
                .limit(maxKeys)
                .toList();
    }

    private List<ObjectInfo> fetchListFromAnyNode(String bucket, String query, List<InetSocketAddress> nodes) {
        // DOWN nodes are skipped entirely. If none are healthy, return an empty list
        // — no fallback retries against nodes the tracker has written off.
        List<InetSocketAddress> healthy = nodes.stream()
                .filter(healthTracker::isHealthy)
                .toList();

        for (InetSocketAddress node : healthy) {
            URI uri = URI.create(String.format("http://%s:%d/bucket/%s%s",
                    node.getHostString(), node.getPort(),
                    URLEncoder.encode(bucket, java.nio.charset.StandardCharsets.UTF_8), query));
            HttpRequest req = HttpRequest.newBuilder().uri(uri).GET().build();
            try {
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    healthTracker.recordSuccess(node);
                    return parseObjectListJson(resp.body());
                }
                logger.trace("GET list {} → node {} HTTP {}", bucket, node, resp.statusCode());
            } catch (Exception e) {
                healthTracker.recordFailure(node);
                logger.trace("GET list {} → node {} failed: {}", bucket, node, e.getMessage());
            }
        }
        return List.of();
    }

    /**
     * Parses the storage node's list response: [{"key":"..."},...] using Jackson.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Package-private for testing.
    static List<ObjectInfo> parseObjectListJson(String body) {
        if (body == null || body.isBlank())
            return List.of();
        try {
            List<ObjectInfo> result = OBJECT_MAPPER.readValue(body, new TypeReference<List<ObjectInfo>>() {
            });
            return result != null ? result : List.of();
        } catch (Exception e) {
            logger.warn("Failed to parse object list JSON (body length={}): {}",
                    body != null ? body.length() : 0, e.getMessage());
            return List.of();
        }
    }

    public record ObjectInfo(String key, long size, String lastModified) {
    }

    public boolean beginMultipartCommit(String bucket, String key, String uploadId, List<String> chunks,
            long totalSize) {
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");
        if (uploadId == null)
            throw new IllegalArgumentException("uploadId is null");
        if (chunks == null)
            throw new IllegalArgumentException("chunks is null");

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

        int multipartQuorum = erasureSetOpt.get().getK() + 1;
        return commitCoordinator.beginMultipartCommit(requestId, partition.getAsInt(), bucket, key, chunks, totalSize,
                multipartQuorum);
    }

    public void shutdown() {
        executor.shutdownNow();
        commitCoordinator.close();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the partition and {@link ErasureSetConfiguration.ErasureSet} for
     * the given routing key, returning an empty Optional if routing fails.
     * Used by delete/bucket operations to avoid repeating the same lookup logic.
     */
    private record PartitionAndSet(int partition, ErasureSetConfiguration.ErasureSet erasureSet) {
    }

    private Optional<PartitionAndSet> resolveErasureSet(String routingKey) {
        ErasureRouting r = getRouting();
        OptionalInt partition = r.getPartition(routingKey);
        if (partition.isEmpty())
            return Optional.empty();
        return r.getErasureSet(partition.getAsInt())
                .map(es -> new PartitionAndSet(partition.getAsInt(), es));
    }

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

    /**
     * Holds a raw {@link CompletableFuture} returned by
     * {@link HttpClient#sendAsync} along
     * with the target node it addresses. The raw future (not a {@code thenApply}
     * derivative)
     * must be the cancellation target so that the underlying HTTP exchange is torn
     * down.
     */
    private record InflightTransfer(InetSocketAddress node, CompletableFuture<?> future, int shardIndex) {
    }

    /**
     * Spawns a watcher on the shared virtual-thread executor that periodically
     * checks the
     * health of each in-flight transfer's target node. If the
     * {@link NodeHealthTracker} marks
     * a target as unhealthy, {@code cancel(true)} is invoked on the corresponding
     * raw future,
     * tearing down the connection so a stalled transfer cannot block the parent
     * commit.
     *
     * <p>
     * The watcher exits as soon as every transfer is done (success, failure, or
     * cancelled).
     */
    private void startHealthWatcher(List<InflightTransfer> inflight) {
        if (inflight.isEmpty())
            return;
        executor.execute(() -> {
            try {
                while (true) {
                    boolean anyPending = false;
                    for (InflightTransfer t : inflight) {
                        if (t.future().isDone())
                            continue;
                        anyPending = true;
                        if (!healthTracker.isHealthy(t.node())) {
                            logger.trace("Cancelling in-flight shard {} transfer to {} (marked DOWN)",
                                    t.shardIndex(), t.node());
                            t.future().cancel(true);
                        }
                    }
                    if (!anyPending)
                        return;
                    Thread.sleep(HEALTH_WATCH_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Reads and discards every byte from {@code stream}, then closes it. Used to
     * keep
     * {@link ErasureCoder#shard} producer pipes draining for shards we deliberately
     * skip
     * (DOWN nodes) so the producer never stalls on a full queue.
     */
    private static void drainAndClose(InputStream stream) {
        try (InputStream s = stream) {
            byte[] buf = new byte[64 * 1024];
            while (s.read(buf) >= 0) {
                // discard
            }
        } catch (IOException ignored) {
            // The producer side will surface its own error to the healthy pipes if any.
        }
    }

    /**
     * Unwraps a {@link CompletionException} (used by {@link CompletableFuture}'s
     * {@code handle})
     * to expose the underlying cause — typically a {@link CancellationException}
     * when the
     * health watcher tore down the request.
     */
    private static Throwable unwrap(Throwable ex) {
        if (ex instanceof CompletionException && ex.getCause() != null) {
            return ex.getCause();
        }
        return ex;
    }

    private List<InetSocketAddress> getNodesForPartition(int partition) {
        ErasureRouting r = getRouting();
        Optional<ErasureSetConfiguration.ErasureSet> esOpt = r.getErasureSet(partition);
        if (esOpt.isEmpty()) {
            logger.error("No erasure set found for partition {}", partition);
            return List.of();
        }
        return esOpt.get().getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort()))
                .toList();
    }

}