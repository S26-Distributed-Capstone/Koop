package com.github.koop.storagenode;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.ErasureRouting;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.pubsub.CommitTopics;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;

import io.javalin.Javalin;
import io.javalin.http.Context;

public class StorageNodeServerV2 {

    private final int port;
    private final String ip;
    private final StorageNodeV2 storageNode;
    private final MetadataClient metadataClient;
    private final PubSubClient pubSubClient;
    private final RepairWorkerPool repairWorkerPool;
    private final Database db;

    private Javalin app;
    private ErasureSetConfiguration currentEsConfig;
    private PartitionSpreadConfiguration currentPsConfig;
    private volatile int myShardIndex = -1;

    private final Set<Integer> subscribedPartitions = new HashSet<>();
    private final Map<Integer, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Logger logger = LogManager.getLogger(StorageNodeServerV2.class);

    public StorageNodeServerV2(int port, String ip, Database db, Path dir, MetadataClient metadataClient,
            PubSubClient pubSubClient) {
        this.port = port;
        this.ip = ip;
        this.db = db;
        WriteTracker writeTracker = new WriteTracker();
        // Lambda captures 'this' — storageNode/config are dereferenced at call time,
        // not at construction time, so there is no circular dependency.
        this.repairWorkerPool = new RepairWorkerPool(writeTracker, this::repairBlob);
        this.storageNode = new StorageNodeV2(db, dir, writeTracker);
        this.metadataClient = metadataClient;
        this.pubSubClient = pubSubClient;
    }

    public static void main(String[] args) {
        String envPort = System.getenv("PORT");
        String envDir = System.getenv("STORAGE_DIR");
        String envIp = System.getenv("NODE_IP");

        int port = (envPort != null) ? Integer.parseInt(envPort) : 8080;
        String ip = (envIp != null) ? envIp : "127.0.0.1";
        Path storagePath = Path.of((envDir != null) ? envDir : "./storage");

        logger.info("Starting StorageNodeServerV2 with IP={}, port={} and storagePath={}", ip, port, storagePath);

        try {
            java.nio.file.Files.createDirectories(storagePath);
        } catch (IOException e) {
            logger.error("Failed to create storage directory: {}", storagePath);
            System.exit(1);
        }

        Database db = null;
        MetadataClient metadataClient = null;
        PubSubClient pubSubClient = null;

        StorageNodeServerV2 server = new StorageNodeServerV2(port, ip, db, storagePath, metadataClient, pubSubClient);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server...");
            server.stop();
        }));

        server.start();
    }

    private synchronized void updateSubscriptions() {
        if (currentEsConfig == null || currentPsConfig == null) {
            return;
        }

        int myErasureSetId = -1;
        int newShardIndex = -1;

        outer:
        for (ErasureSetConfiguration.ErasureSet es : currentEsConfig.getErasureSets()) {
            List<ErasureSetConfiguration.Machine> machines = es.getMachines();
            for (int i = 0; i < machines.size(); i++) {
                ErasureSetConfiguration.Machine machine = machines.get(i);
                if (machine.getIp().equals(this.ip) && machine.getPort() == app.port()) {
                    myErasureSetId = es.getNumber();
                    newShardIndex = i;
                    break outer;
                }
            }
        }

        this.myShardIndex = newShardIndex;

        Set<Integer> targetPartitions = new HashSet<>();

        if (myErasureSetId == -1) {
            logger.warn("Node {}:{} not found in any Erasure Set. Dropping all partition subscriptions.", this.ip,
                    app.port());
        } else {
            for (PartitionSpreadConfiguration.PartitionSpread spread : currentPsConfig.getPartitionSpread()) {
                if (spread.getErasureSet() == myErasureSetId) {
                    targetPartitions.addAll(spread.getPartitions());
                }
            }
        }

        Set<Integer> toDrop = new HashSet<>(subscribedPartitions);
        toDrop.removeAll(targetPartitions);

        for (Integer partition : toDrop) {
            String topic = "partition-" + partition;
            logger.info("Node unassigned from partition {}. Dropping subscription for topic: {}", partition, topic);
            pubSubClient.drop(topic);
            subscribedPartitions.remove(partition);

            ExecutorService executor = partitionExecutors.remove(partition);
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        Set<Integer> toAdd = new HashSet<>(targetPartitions);
        toAdd.removeAll(subscribedPartitions);

        for (Integer partition : toAdd) {
            String topic = CommitTopics.forPartition(partition);
            logger.info("Node assigned to partition {}. Subscribing to topic: {}",
                    partition, topic);

            ExecutorService partitionExecutor = Executors.newSingleThreadExecutor(
                    Thread.ofVirtual().name("partition-" + partition + "-").factory());
            partitionExecutors.put(partition, partitionExecutor);

            pubSubClient.sub(topic,(incomingTopic, offset, messageBytes) -> {
                partitionExecutor.submit(() -> {
                    try {
                        Message message = Message.deserializeMessage(messageBytes);
                        processSequencerMessage(partition, message, offset);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize or process message on topic {} at offset {}", incomingTopic,
                                offset, e);
                    }
                });
            });

            subscribedPartitions.add(partition);
        }
    }

   private void handlePut(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String fullKey = bucket + "/" + key;
            String requestId = ctx.queryParam("requestId");

            logger.debug("Received PUT request: partition={} fullKey={} requestId={}", partition, fullKey,
                    requestId);

            if (requestId == null || requestId.isBlank()) {
                ctx.status(400).result("Missing requestId parameter");
                return;
            }

            storageNode.store(partition, fullKey, requestId, Channels.newChannel(ctx.bodyInputStream()));

            ctx.status(200).result("OK");
            logger.debug("PUT (Uncommitted) partition={} fullKey={} requestId={}", partition, fullKey, requestId);
        } catch (Exception e) {
            logger.error("Error handling PUT", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleGet(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String fullKey = bucket + "/" + key;

            String versionParam = ctx.queryParam("version");
            long targetVersion = -1;

            if (versionParam != null && !versionParam.isBlank()) {
                try {
                    targetVersion = Long.parseLong(versionParam);
                } catch (NumberFormatException e) {
                    ctx.status(400).result("Invalid version parameter");
                    return;
                }
            }
            logger.debug("GET partition={} fullKey={} targetVersion={}", partition, fullKey, targetVersion);

            Optional<StorageNodeV2.GetObjectResponse> dataOpt = storageNode.retrieve(fullKey, targetVersion);

            if (dataOpt.isPresent()) {
                StorageNodeV2.GetObjectResponse response = dataOpt.get();

                long responseSequenceNumber = switch (response) {
                    case StorageNodeV2.FileObject fo -> fo.version().sequenceNumber();
                    case StorageNodeV2.MultipartData md -> md.version().sequenceNumber();
                    case StorageNodeV2.Tombstone t -> t.version().sequenceNumber();
                };

                ctx.header("X-Koop-Version", String.valueOf(responseSequenceNumber));

                if (response instanceof StorageNodeV2.FileObject fo) {
                    ctx.header("X-Koop-Type", "BLOB");
                    var fc = fo.data();
                    ctx.status(200)
                            .header("Content-Type", "application/octet-stream")
                            .header("Content-Length", String.valueOf(fc.size()));

                    var outputChannel = Channels.newChannel(ctx.res().getOutputStream());
                    long size = fc.size();
                    logger.debug("GET partition={} fullKey={} version={} streaming {} bytes", partition, fullKey,
                            responseSequenceNumber, size);
                    long position = 0L;
                    while (position < size) {
                        long transferred = fc.transferTo(position, size - position, outputChannel);
                        if (transferred <= 0) {
                            logger.warn(
                                    "Zero-byte transfer when streaming file for partition={} fullKey={} at position={} of {} bytes",
                                    partition, fullKey, position, size);
                            break;
                        }
                        position += transferred;
                    }
                    fo.close();
                    logger.debug("GET partition={} fullKey={} version={} streamed {} bytes", partition, fullKey,
                            responseSequenceNumber, size);

                } else if (response instanceof StorageNodeV2.MultipartData md) {
                    ctx.header("X-Koop-Type", "MULTIPART");
                    ctx.status(200)
                       .header("Content-Type", "application/json")
                       .json(md.version().chunks());
                    logger.debug("GET partition={} fullKey={} version={} returned multipart chunks in body", partition, fullKey,
                            responseSequenceNumber);

                } else if (response instanceof StorageNodeV2.Tombstone) {
                    ctx.header("X-Koop-Type", "TOMBSTONE");
                    ctx.status(200).result("");
                    logger.debug("GET partition={} fullKey={} version={} hit tombstone", partition, fullKey,
                            responseSequenceNumber);
                }
            } else {
                ctx.status(404).result("NOT_FOUND");
                logger.debug("GET partition={} fullKey={} targetVersion={} not found", partition, fullKey, targetVersion);
            }
        } catch (Exception e) {
            logger.error("Error handling GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    public void processSequencerMessage(int partition, Message message, long seqNumber) {
        try {
            String requestId = null;
            String callbackHost = message.sender().getHostString();
            int callbackPort = message.sender().getPort();
            String callbackAddress = "http://" + callbackHost + ":" + callbackPort;

            switch (message) {
                case Message.FileCommitMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    boolean materialized = storageNode.commit(partition, fullKey, m.requestID(), seqNumber);
                    if (!materialized) {
                        repairWorkerPool.enqueue(new RepairOperation(fullKey, seqNumber, m.requestID()));
                    }
                    requestId = m.requestID();
                    logger.info("Committed file: {}", fullKey);
                }
                case Message.MultipartCommitMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.multipartCommit(partition, fullKey, seqNumber, m.chunks());
                    requestId = m.requestID();
                    logger.info("Committed multipart file: {}", fullKey);
                }
                case Message.DeleteMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.delete(partition, fullKey, seqNumber);
                    requestId = m.requestID();
                    logger.info("Deleted file (Tombstone): {}", fullKey);
                }
                case Message.CreateBucketMessage m -> {
                    storageNode.createBucket(partition, m.bucket(), seqNumber);
                    requestId = m.requestID();
                    logger.info("Created bucket: {}", m.bucket());
                }
                case Message.DeleteBucketMessage m -> {
                    storageNode.deleteBucket(partition, m.bucket(), seqNumber);
                    requestId = m.requestID();
                    logger.info("Deleted bucket: {}", m.bucket());
                }
            }

            sendAck(callbackAddress, requestId);

        } catch (Exception e) {
            logger.error("Failed to process sequencer message: " + message, e);
        }
    }

    // -------------------------------------------------------------------------
    // Repair logic — implements BlobRepairStrategy via method reference
    // -------------------------------------------------------------------------

    /**
     * Fetches a missing shard from peer nodes and writes it to local disk.
     *
     * <p>This method is passed as the {@link BlobRepairStrategy} to
     * {@link RepairWorkerPool} via a method reference ({@code this::repairBlob}).
     * It runs on a virtual-thread worker managed by the pool.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Determine the partition for the blob key using ErasureRouting (CRC32 hash).</li>
     *   <li>Find the erasure set responsible for that partition.</li>
     *   <li>Determine this node's shard index within the erasure set.</li>
     *   <li>Fetch shards from peer nodes (excluding self).</li>
     *   <li>Reconstruct all shards (including this node's) via Reed-Solomon.</li>
     *   <li>Extract this node's shard and write it to disk.</li>
     * </ol>
     */
    private void repairBlob(RepairOperation operation) {
        String blobKey = operation.blobKey();
        long seqOffset = operation.seqOffset();

        logger.info("Starting repair: key={}, seqOffset={}", blobKey, seqOffset);

        if (currentEsConfig == null || currentPsConfig == null) {
            logger.warn("Repair skipped: metadata config not yet available for key={}", blobKey);
            return;
        }

        ErasureRouting routing = new ErasureRouting(currentPsConfig, currentEsConfig);

        // 1. Determine partition via CRC32 hash (same as StorageWorker)
        var partitionOpt = routing.getPartition(blobKey);
        if (partitionOpt.isEmpty()) {
            logger.error("Repair failed: cannot determine partition for key={}", blobKey);
            return;
        }
        int partition = partitionOpt.getAsInt();

        // 2. Find the erasure set
        var esOpt = routing.getErasureSet(partition);
        if (esOpt.isEmpty()) {
            logger.error("Repair failed: no erasure set for partition={} key={}", partition, blobKey);
            return;
        }
        ErasureSetConfiguration.ErasureSet es = esOpt.get();
        List<ErasureSetConfiguration.Machine> machines = es.getMachines();
        int n = es.getN();
        int k = es.getK();

        // 3. Determine this node's shard index (cached from last metadata update)
        int myIndex = this.myShardIndex;
        if (myIndex == -1) {
            logger.error("Repair failed: this node {}:{} not found in erasure set {} for key={}",
                    this.ip, app.port(), es.getNumber(), blobKey);
            return;
        }

        // 4. Fetch shards from peers
        InputStream[] shardStreams = new InputStream[n];
        boolean[] present = new boolean[n];

        List<Callable<ShardFetchResult>> fetchTasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (i == myIndex) continue; // skip self — we're the one missing the shard
            final int idx = i;
            ErasureSetConfiguration.Machine peer = machines.get(i);
            fetchTasks.add(() -> fetchShardFromPeer(peer, partition, blobKey, seqOffset, idx));
        }

        ExecutorService fetchExecutor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var futures = fetchExecutor.invokeAll(fetchTasks, 30, TimeUnit.SECONDS);
            int fetched = 0;
            for (var future : futures) {
                try {
                    ShardFetchResult result = future.get();
                    if (result != null && result.data != null) {
                        shardStreams[result.nodeIndex] = result.data;
                        present[result.nodeIndex] = true;
                        fetched++;
                    }
                } catch (Exception e) {
                    logger.debug("Failed to get shard fetch result", e);
                }
            }

            if (fetched < k) {
                logger.error("Repair failed: only {}/{} shards fetched for key={}", fetched, k, blobKey);
                return;
            }

            // 5. Reconstruct via erasure coding
            try (InputStream reconstructed = ErasureCoder.reconstruct(shardStreams, present, k, n)) {
                // The reconstructed stream is the original data. We need to re-shard it
                // to extract just our shard.
                byte[] fullData = reconstructed.readAllBytes();

                // Re-shard to get this node's shard
                InputStream[] resharded = ErasureCoder.shard(
                        new java.io.ByteArrayInputStream(fullData),
                        fullData.length, k, n);

                // Extract our shard
                byte[] ourShard = resharded[myIndex].readAllBytes();

                // 6. Write our shard to disk
                storageNode.store(partition, blobKey, operation.requestId(),
                        Channels.newChannel(new java.io.ByteArrayInputStream(ourShard)));

                logger.info("Repair succeeded: key={}, seqOffset={}, shard index={}, {} bytes written",
                        blobKey, seqOffset, myIndex, ourShard.length);
            }
        } catch (Exception e) {
            logger.error("Repair failed for key={}: {}", blobKey, e.getMessage(), e);
        } finally {
            fetchExecutor.shutdownNow();
            // Close any open shard streams
            for (InputStream is : shardStreams) {
                if (is != null) {
                    try { is.close(); } catch (IOException ignored) {}
                }
            }
        }
    }

    private record ShardFetchResult(int nodeIndex, InputStream data) {}

    private ShardFetchResult fetchShardFromPeer(ErasureSetConfiguration.Machine peer,
            int partition, String blobKey, long seqOffset, int nodeIndex) {
        try {
            URI uri = new URI("http", null, peer.getIp(), peer.getPort(),
                    "/store/" + partition + "/" + blobKey, "version=" + seqOffset, null);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<InputStream> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() == 200) {
                String type = response.headers().firstValue("X-Koop-Type").orElse("BLOB");
                if ("BLOB".equals(type)) {
                    logger.debug("Fetched shard {} from peer {}:{} for key={}",
                            nodeIndex, peer.getIp(), peer.getPort(), blobKey);
                    return new ShardFetchResult(nodeIndex, response.body());
                }
            }
            logger.debug("Peer {}:{} returned status {} for key={}",
                    peer.getIp(), peer.getPort(), response.statusCode(), blobKey);
        } catch (Exception e) {
            logger.debug("Failed to fetch shard from peer {}:{} for key={}: {}",
                    peer.getIp(), peer.getPort(), blobKey, e.getMessage());
        }
        return null;
    }

    // -------------------------------------------------------------------------

    private void sendAck(String callbackAddress, String requestId) {
        if (callbackAddress == null || callbackAddress.isBlank() || requestId == null || requestId.isBlank()) {
            return;
        }

        try {
            String baseUrl = callbackAddress.endsWith("/") ? 
                    callbackAddress.substring(0, callbackAddress.length() - 1) : callbackAddress;
            String url = baseUrl + "/ack/" + requestId;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .thenAccept(res -> {
                        if (res.statusCode() >= 200 && res.statusCode() < 300) {
                            logger.debug("Successfully sent ACK for requestId: {} to {}", requestId, url);
                        } else {
                            logger.warn("Received non-2xx status {} when sending ACK for requestId: {} to {}",
                                    res.statusCode(), requestId, url);
                        }
                    })
                    .exceptionally(ex -> {
                        logger.error("Network error while sending ACK for requestId: {} to {}", requestId, url, ex);
                        return null;
                    });
        } catch (IllegalArgumentException e) {
            logger.error("Invalid callback URL format: {} for requestId: {}", callbackAddress, requestId, e);
        }
    }

    private String buildKey(String bucket, String key) {
        if (bucket == null || bucket.isEmpty())
            return key;
        return bucket + "/" + key;
    }

    public void start() {
        repairWorkerPool.start();

        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.put("/store/{partition}/{bucket}/<key>", this::handlePut);
            config.routes.get("/store/{partition}/{bucket}/<key>", this::handleGet);
        });

        app.start(port);
        logger.info("StorageNodeServerV2 started on port {}", app.port());

        if (metadataClient != null) {
            metadataClient.listen(ErasureSetConfiguration.class, (prev, current) -> {
                this.currentEsConfig = current;
                updateSubscriptions();
            });

            metadataClient.listen(PartitionSpreadConfiguration.class, (prev, current) -> {
                this.currentPsConfig = current;
                updateSubscriptions();
            });

            try {
                this.currentEsConfig = metadataClient.waitFor(ErasureSetConfiguration.class, 30, TimeUnit.SECONDS);
                this.currentPsConfig = metadataClient.waitFor(PartitionSpreadConfiguration.class, 30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for initial metadata");
            }

            updateSubscriptions();
        }
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }

        repairWorkerPool.shutdown();

        partitionExecutors.values().forEach(ExecutorService::shutdownNow);
        partitionExecutors.clear();

        logger.info("StorageNodeServerV2 stopped");
    }

    public int port() {
        return app != null ? app.port() : port;
    }

    /** Package-private accessor for tests to inspect pending repair count. */
    int repairPendingCount() {
        return repairWorkerPool.pendingCount();
    }
}