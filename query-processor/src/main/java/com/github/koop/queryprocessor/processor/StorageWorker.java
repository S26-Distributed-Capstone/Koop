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

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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

    // Convenience constructor — creates a MemoryPubSub-backed CommitCoordinator
    // on an OS-assigned port. Useful when only a MetadataClient is available.
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
            return null;
        }

        int resolvedPartition = partition.getAsInt();
        ErasureSetConfiguration.ErasureSet es = erasureSetOpt.get();
        var resolvedNodes = es.getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        int k = es.getK();
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
                    handleMultipartGet(representative.chunks(), k, pos);
                } catch (Exception e) {
                    logger.error("Failed to stream multipart data for key {}", storageKey, e);
                }
            });
        } else {
            // BLOB — reconstruct from erasure-coded shards
            executor.execute(() -> {
                try (pos) {
                    if (maxVersionResponses.size() >= k) {
                        reconstructFromResponses(maxVersionResponses, resolvedNodes, k, n, pos);
                    } else {
                        reconstructFromOlderVersion(resolvedPartition, storageKey, resolvedNodes, k, n, pos,
                                maxVersion, maxVersionResponses.size(), responses);
                    }
                } catch (Exception e) {
                    logger.error("Failed to reconstruct stream for key {}", storageKey, e);
                }
            });
        }

        return pis;
    }

    private void processGetConsensus(int partition, String storageKey, List<InetSocketAddress> nodes, int k, int n,
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
            logger.debug("Latest version {} for key {} is a tombstone.", maxVersion, storageKey);
            throw new IOException("Object not found (deleted)");
        }

        if (representative.type() == ShardType.MULTIPART) {
            logger.debug("Latest version {} for key {} is multipart. Streaming chunks sequentially.", maxVersion,
                    storageKey);
            handleMultipartGet(representative.chunks(), k, out);
            return;
        }

        // Type is BLOB
        if (maxVersionResponses.size() >= k) {
            reconstructFromResponses(maxVersionResponses, nodes, k, n, out);
            return;
        }

        reconstructFromOlderVersion(partition, storageKey, nodes, k, n, out, maxVersion, maxVersionResponses.size(),
                responses);
    }

    private void reconstructFromOlderVersion(int partition, String storageKey, List<InetSocketAddress> nodes, int k,
            int n,
            OutputStream out, long maxVersion, int maxVersionShardCount, List<ShardResponse> responses)
            throws IOException {
        logger.warn("Latest version {} for key {} has insufficient shards ({}/{}). Searching older versions.",
                maxVersion, storageKey, maxVersionShardCount, k);

        List<Long> availableVersions = responses.stream()
                .map(ShardResponse::version)
                .distinct()
                .sorted(Comparator.reverseOrder())
                .toList();

        if (availableVersions.size() < 2) {
            throw new IOException("Only one version available for key " + storageKey
                    + " and it has insufficient shards (" + maxVersionShardCount + "/" + k + ")");
        }

        long oldVersion = availableVersions.get(1); // second-highest version
        List<ShardResponse> oldResponses = fetchShards(partition, storageKey, nodes, OptionalLong.of(oldVersion));
        List<ShardResponse> validOldResponses = oldResponses.stream().filter(r -> r.version() == oldVersion).toList();
        if (validOldResponses.size() >= k) {
            logger.debug("Found sufficient shards for version {} ({}/{}). Reconstructing.", oldVersion,
                    validOldResponses.size(), k);
            reconstructFromResponses(validOldResponses, nodes, k, n, out);
            return;
        }

        throw new IOException("Could not find any version with sufficient shards for key " + storageKey);
    }

    private void handleMultipartGet(List<String> chunks, int k, OutputStream out) throws IOException {
        for (String chunkId : chunks) {
            var partition = getRouting().getPartition(chunkId)
                    .orElseThrow(() -> new IOException("No partition for chunk " + chunkId));
            List<InetSocketAddress> nodes = getNodesForPartition(partition);
            processGetConsensus(partition, chunkId, nodes, k, nodes.size(), out);
        }
    }

    private List<ShardResponse> fetchShards(int partition, String storageKey, List<InetSocketAddress> nodes,
            OptionalLong targetVersion) {
        List<Callable<ShardResponse>> tasks = new ArrayList<>();

        for (int i = 0; i < nodes.size(); i++) {
            final int index = i;
            tasks.add(() -> {
                InetSocketAddress node = nodes.get(index);
                String uriStr = String.format("http://%s:%d/store/%d/%s", node.getHostString(), node.getPort(),
                        partition, storageKey);
                if (targetVersion.isPresent()) {
                    uriStr += "?version=" + targetVersion.getAsLong();
                }

                HttpRequest request = HttpRequest.newBuilder().uri(URI.create(uriStr)).GET().build();
                HttpResponse<InputStream> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofInputStream());

                if (response.statusCode() == 200) {
                    long version = response.headers().firstValueAsLong("X-Koop-Version").orElse(-1L);
                    ShardType type = ShardType.fromHeader(response.headers().firstValue("X-Koop-Type").orElse("BLOB"));
                    List<String> chunks = new ArrayList<>();
                    if (type == ShardType.MULTIPART) {
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
                        return new ShardResponse(index, version, type, chunks, InputStream.nullInputStream());
                    }

                    return new ShardResponse(index, version, type, List.of(), response.body());
                }
                return null;
            });
        }

        try {
            return executor.invokeAll(tasks).stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(res -> res != null)
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    private void reconstructFromResponses(List<ShardResponse> payloadResponses, List<InetSocketAddress> nodes, int k,
            int n, OutputStream out) throws IOException {
        InputStream[] ins = new InputStream[n];
        boolean[] present = new boolean[n];

        for (ShardResponse res : payloadResponses) {
            if (res.nodeIndex() < n) {
                ins[res.nodeIndex()] = res.payload();
                present[res.nodeIndex()] = true;
            }
        }

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present, k, n)) {
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

    private record ShardResponse(int nodeIndex, long version, ShardType type, List<String> chunks,
            InputStream payload) {
    }

    /**
     * Publishes a delete command via pub/sub and waits for {@code k + 1} SNs
     * (the delete quorum) to acknowledge the tombstone.
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
     * <p>This prevents false negatives when a node is stale or partitioned:
     * even a single node reporting the bucket exists is sufficient.
     */
    public boolean bucketExists(String bucket) {
        if (bucket == null) throw new IllegalArgumentException("bucket is null");

        Optional<PartitionAndSet> resolved = resolveErasureSet(bucket);
        if (resolved.isEmpty()) {
            logger.error("Routing failed for bucket {}, aborting bucketExists", bucket);
            return false;
        }

        List<InetSocketAddress> nodes = resolved.get().erasureSet().getMachines().stream()
                .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (InetSocketAddress node : nodes) {
            tasks.add(() -> {
                URI uri = URI.create(String.format("http://%s:%d/bucket/%s",
                        node.getHostString(), node.getPort(), bucket));
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(uri)
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .build();
                try {
                    HttpResponse<Void> resp = httpClient.send(req, HttpResponse.BodyHandlers.discarding());
                    int status = resp.statusCode();
                    if (status == 200) return true;
                    if (status == 404) {
                        logger.trace("HEAD bucket {} → node {} HTTP 404, trying next", bucket, node);
                        return false;
                    }
                    logger.trace("HEAD bucket {} → node {} HTTP {}, trying next", bucket, node, status);
                } catch (Exception e) {
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
        if (bucket == null) throw new IllegalArgumentException("bucket is null");
        if (maxKeys < 0) throw new IllegalArgumentException("maxKeys < 0");

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
        String query = "?maxKeys=" + maxKeys
                + (safePrefix.isEmpty() ? "" : "&prefix=" + safePrefix);

        List<Callable<List<ObjectInfo>>> tasks = new ArrayList<>();
        for (int partition : partitions) {
            Optional<ErasureSetConfiguration.ErasureSet> esOpt = r.getErasureSet(partition);
            if (esOpt.isEmpty()) continue;
            List<InetSocketAddress> nodes = esOpt.get().getMachines().stream()
                    .map(m -> new InetSocketAddress(m.getIp(), m.getPort())).toList();
            if (nodes.isEmpty()) continue;
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
        for (InetSocketAddress node : nodes) {
            URI uri = URI.create(String.format("http://%s:%d/bucket/%s%s",
                    node.getHostString(), node.getPort(), bucket, query));
            HttpRequest req = HttpRequest.newBuilder().uri(uri).GET().build();
            try {
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    return parseObjectListJson(resp.body());
                }
                logger.trace("GET list {} → node {} HTTP {}", bucket, node, resp.statusCode());
            } catch (Exception e) {
                logger.trace("GET list {} → node {} failed: {}", bucket, node, e.getMessage());
            }
        }
        return List.of();
    }

    /** Parses the storage node's list response: [{"key":"..."},...]. */
    private static List<ObjectInfo> parseObjectListJson(String body) {
        if (body == null) return List.of();
        String trimmed = body.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) return List.of();
        String inner = trimmed.substring(1, trimmed.length() - 1).trim();
        if (inner.isEmpty()) return List.of();

        List<ObjectInfo> out = new ArrayList<>();
        int depth = 0;
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '{') depth++;
            if (c == '}') depth--;
            if (c == ',' && depth == 0) {
                ObjectInfo oi = parseObjectEntry(cur.toString());
                if (oi != null) out.add(oi);
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) {
            ObjectInfo oi = parseObjectEntry(cur.toString());
            if (oi != null) out.add(oi);
        }
        return out;
    }

    private static ObjectInfo parseObjectEntry(String entry) {
        int keyIdx = entry.indexOf("\"key\"");
        if (keyIdx < 0) return null;
        int firstQuote = entry.indexOf('"', entry.indexOf(':', keyIdx) + 1);
        if (firstQuote < 0) return null;
        StringBuilder key = new StringBuilder();
        boolean escape = false;
        int i = firstQuote + 1;
        for (; i < entry.length(); i++) {
            char c = entry.charAt(i);
            if (escape) { key.append(c); escape = false; continue; }
            if (c == '\\') { escape = true; continue; }
            if (c == '"') break;
            key.append(c);
        }
        long size = parseLongField(entry, "size");
        String lastModified = parseStringField(entry, "lastModified");
        return new ObjectInfo(key.toString(), size, lastModified);
    }

    private static long parseLongField(String entry, String field) {
        int idx = entry.indexOf("\"" + field + "\"");
        if (idx < 0) return 0L;
        int colon = entry.indexOf(':', idx);
        if (colon < 0) return 0L;
        StringBuilder num = new StringBuilder();
        for (int i = colon + 1; i < entry.length(); i++) {
            char c = entry.charAt(i);
            if (Character.isDigit(c) || c == '-') num.append(c);
            else if (num.length() > 0) break;
        }
        try {
            return num.length() == 0 ? 0L : Long.parseLong(num.toString());
        } catch (NumberFormatException e) { return 0L; }
    }

    private static String parseStringField(String entry, String field) {
        int idx = entry.indexOf("\"" + field + "\"");
        if (idx < 0) return "";
        int firstQuote = entry.indexOf('"', entry.indexOf(':', idx) + 1);
        if (firstQuote < 0) return "";
        StringBuilder val = new StringBuilder();
        boolean escape = false;
        for (int i = firstQuote + 1; i < entry.length(); i++) {
            char c = entry.charAt(i);
            if (escape) { val.append(c); escape = false; continue; }
            if (c == '\\') { escape = true; continue; }
            if (c == '"') break;
            val.append(c);
        }
        return val.toString();
    }

    public record ObjectInfo(String key, long size, String lastModified) {}

    public boolean beginMultipartCommit(String bucket, String key, String uploadId, List<String> chunks) {
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

        return commitCoordinator.beginMultipartCommit(requestId, partition.getAsInt(), bucket, key, chunks,
                erasureSetOpt.get().getWriteQuorum());
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