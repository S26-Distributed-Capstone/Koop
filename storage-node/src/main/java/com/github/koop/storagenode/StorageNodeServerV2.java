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

import com.github.koop.common.metadata.*;
import com.github.koop.storagenode.db.*;
import com.github.koop.storagenode.gc.ActiveReadTracker;
import com.github.koop.storagenode.gc.BlobDeletionWorker;
import com.github.koop.storagenode.gc.GarbageCollectorWorker;
import com.github.koop.storagenode.gc.GossipService;
import com.github.koop.storagenode.gc.PartitionWatermarks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.Message;
import com.github.koop.common.pubsub.CommitTopics;
import com.github.koop.common.pubsub.PubSubClient;

import io.javalin.Javalin;
import io.javalin.http.Context;

public class StorageNodeServerV2 {

    private final int port;
    private final String ip;
    private final StorageNodeV2 storageNode;
    private final MetadataClient metadataClient;
    private final PubSubClient pubSubClient;
    private final RepairWorkerPool repairWorkerPool;
    private final RocksDbRepairQueue repairQueue;
    private final Database db;
    private final ActiveReadTracker activeReads;
    private final PartitionWatermarks watermarks;
    private final GossipService gossipService;
    private final GarbageCollectorWorker gcWorker;
    private final BlobDeletionWorker blobDeletionWorker;
    private final ErasureCoder erasureCoder;

    /** Eviction window: peers silent for longer than this are excluded from the watermark. */
    static final long GOSSIP_STALE_AFTER_MS = 30_000L;
    /** Frequency of outbound gossip broadcasts. */
    static final long GOSSIP_INTERVAL_MS = 2_000L;
    /** Frequency of background GC passes. */
    static final long GC_INTERVAL_MS = 10_000L;
    /** Frequency of disk reconciliation passes draining the pending-deletes queue. */
    static final long BLOB_DELETION_INTERVAL_MS = 5_000L;
    /** Maximum duration an active GET lease may remain open before being force-evicted. */
    static final long ACTIVE_READ_MAX_LEASE_MS = 5L * 60L * 1000L;
    /** Frequency of background sweeps that prune expired GET leases. */
    static final long ACTIVE_READ_PRUNE_INTERVAL_MS = 30_000L;

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
                               PubSubClient pubSubClient, RocksDbRepairQueue repairQueue) {
        this.port = port;
        this.ip = ip;
        this.db = db;
        this.repairQueue = repairQueue;
        WriteTracker writeTracker = new WriteTracker();
        this.repairWorkerPool = new RepairWorkerPool(repairQueue, writeTracker, this::repairBlob);
        this.storageNode = new StorageNodeV2(db, dir, writeTracker);
        this.metadataClient = metadataClient;
        this.pubSubClient = pubSubClient;
        this.activeReads = new ActiveReadTracker(System::currentTimeMillis,
                ACTIVE_READ_MAX_LEASE_MS, ACTIVE_READ_PRUNE_INTERVAL_MS);
        this.watermarks = new PartitionWatermarks();
        String nodeId = ip + ":" + port;
        this.gossipService = new GossipService(nodeId, db, activeReads, watermarks, pubSubClient,
                ignored -> snapshotSubscribedPartitions(), GOSSIP_INTERVAL_MS);
        this.gcWorker = new GarbageCollectorWorker(db, watermarks,
                ignored -> snapshotSubscribedPartitions(), GOSSIP_STALE_AFTER_MS, GC_INTERVAL_MS);
        this.blobDeletionWorker = new BlobDeletionWorker(db, dir, BLOB_DELETION_INTERVAL_MS);
        this.erasureCoder = new ErasureCoder();
    }

    public static void main(String[] args) {
        logger.error("Starting StorageNodeServerV2...");
        String envPort = System.getenv("APP_PORT");
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

        Database db;
        RocksDbRepairQueue repairQueue;
        try {
            String dbPath = storagePath.resolve("rocksdb").toString();
            RocksDbStorageStrategy strategy = new RocksDbStorageStrategy(dbPath);
            repairQueue = new RocksDbRepairQueue(strategy);
            db = new Database(strategy);
            logger.info("RocksDB opened at {}", dbPath);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            logger.error("Failed to open RocksDB", e);
            System.exit(1);
            return;
        }

        logger.info(">>> Creating MetadataClient");
        Map<Class<?>, String> metadataKeys = Map.of(
                ErasureSetConfiguration.class,    "erasure_set_configurations",
                PartitionSpreadConfiguration.class, "partition_spread_configurations"
        );
        MetadataClient metadataClient = new MetadataClient(new EtcdFetcher(metadataKeys));
        logger.info(">>> MetadataClient created");
        metadataClient.start();
        logger.info(">>> MetadataClient started");


        logger.info(">>> Creating PubSubClient");
        String kafkaBrokers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        PubSubClient pubSubClient;
        if (kafkaBrokers != null && !kafkaBrokers.isBlank()) {
            logger.info("Kafka brokers found, using KafkaPubSub: {}", kafkaBrokers);
            pubSubClient = new PubSubClient(new com.github.koop.common.pubsub.KafkaPubSub(kafkaBrokers,
                    "koop-sn-" + ip + "-" + port));
        } else {
            logger.error("No KAFKA_BOOTSTRAP_SERVERS set");
            return;
        }
        pubSubClient.start();
        logger.info(">>> PubSubClient started");


        StorageNodeServerV2 server = new StorageNodeServerV2(port, ip, db, storagePath, metadataClient, pubSubClient, repairQueue);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server...");
            server.stop();
            try {
                db.close();
            } catch (Exception e) {
                logger.error("Failed to close database", e);
            }
        }));

        server.start();
    }

    private synchronized Set<Integer> snapshotSubscribedPartitions() {
        return Set.copyOf(subscribedPartitions);
    }

    private synchronized void updateSubscriptions() {
        if (currentEsConfig == null || currentPsConfig == null) {
            return;
        }

        Set<Integer> myErasureSetIds = new HashSet<>();
        int newShardIndex = -1;

        for (ErasureSetConfiguration.ErasureSet es : currentEsConfig.getErasureSets()) {
            List<ErasureSetConfiguration.Machine> machines = es.getMachines();
            for (int i = 0; i < machines.size(); i++) {
                ErasureSetConfiguration.Machine machine = machines.get(i);
                if (machine.getIp().equals(this.ip) && machine.getPort() == app.port()) {
                    myErasureSetIds.add(es.getNumber());
                    newShardIndex = i;
                    break;
                }
            }
        }

        this.myShardIndex = newShardIndex;

        Set<Integer> targetPartitions = new HashSet<>();

        if (myErasureSetIds.isEmpty()) {
            logger.warn("Node {}:{} not found in any Erasure Set. Dropping all partition subscriptions.", this.ip,
                    app.port());
        } else {
            for (PartitionSpreadConfiguration.PartitionSpread spread : currentPsConfig.getPartitionSpread()) {
                if (myErasureSetIds.contains(spread.getErasureSet())) {
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
            watermarks.forgetPartition(partition);
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
            int urlPartition = Integer.parseInt(ctx.pathParam("partition"));
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

            // Look up the object's metadata first to determine its assigned partition
            // (per the gossip-GC contract — the URL partition is advisory only).
            Optional<Metadata> metaOpt = db.getItem(fullKey);
            if (metaOpt.isEmpty()) {
                ctx.status(404).result("NOT_FOUND");
                logger.debug("GET urlPartition={} fullKey={} not found", urlPartition, fullKey);
                return;
            }
            Metadata meta = metaOpt.get();
            int partition = meta.partition();

            long readSeq;
            if (targetVersion >= 0) {
                // Validate before pinning: an unchecked targetVersion lets a client
                // hold the watermark down on an arbitrarily-low seqNum (e.g. 0)
                // by just keeping the socket open. Refuse versions that don't
                // exist in metadata so the lease tracks a real version.
                final long finalTargetVersion = targetVersion;
                boolean exists = meta.versions().stream()
                        .anyMatch(v -> v.sequenceNumber() == finalTargetVersion);
                if (!exists) {
                    ctx.status(404).result("NOT_FOUND");
                    logger.debug("GET partition={} fullKey={} targetVersion={} no such version",
                            partition, fullKey, targetVersion);
                    return;
                }
                readSeq = targetVersion;
            } else if (!meta.versions().isEmpty()) {
                readSeq = meta.versions().get(meta.versions().size() - 1).sequenceNumber();
            } else {
                ctx.status(404).result("NOT_FOUND");
                return;
            }

            logger.debug("GET partition={} fullKey={} targetVersion={} readSeq={}",
                    partition, fullKey, targetVersion, readSeq);

            try (ActiveReadTracker.Handle ignored = activeReads.begin(partition, readSeq)) {
            Optional<StorageNodeV2.GetObjectResponse> dataOpt = storageNode.retrieve(fullKey, targetVersion);

            if (dataOpt.isPresent()) {
                StorageNodeV2.GetObjectResponse response = dataOpt.get();

                long responseSequenceNumber;
                long logicalSize = -1;
                String typeHeader;

                if (response instanceof StorageNodeV2.FileObject fo) {
                    responseSequenceNumber = fo.version().sequenceNumber();
                    logicalSize = fo.version().size();
                    typeHeader = "BLOB";
                } else if (response instanceof StorageNodeV2.MultipartData md) {
                    responseSequenceNumber = md.version().sequenceNumber();
                    logicalSize = md.version().size();
                    typeHeader = "MULTIPART";
                } else {
                    responseSequenceNumber = ((StorageNodeV2.Tombstone) response).version().sequenceNumber();
                    typeHeader = "TOMBSTONE";
                }

                ctx.header("X-Koop-Version", String.valueOf(responseSequenceNumber));
                ctx.header("X-Koop-Type", typeHeader);

                if (logicalSize >= 0) {
                    ctx.header("X-Koop-Size", String.valueOf(logicalSize));
                }

                if (response instanceof StorageNodeV2.FileObject fo) {
                    var fc = fo.data();
                    ctx.status(200)
                            .header("Content-Type", "application/octet-stream")
                            .header("Content-Length", String.valueOf(fc.size()));

                    var outputChannel = Channels.newChannel(ctx.res().getOutputStream());
                    long physicalSize = fc.size();
                    logger.debug("GET partition={} fullKey={} version={} streaming {} bytes", partition, fullKey,
                            responseSequenceNumber, physicalSize);
                    long position = 0L;
                    while (position < physicalSize) {
                        long transferred = fc.transferTo(position, physicalSize - position, outputChannel);
                        if (transferred <= 0) {
                            break;
                        }
                        position += transferred;
                    }
                    fo.close();
                    logger.debug("GET partition={} fullKey={} version={} streamed {} bytes", partition, fullKey,
                            responseSequenceNumber, physicalSize);

                } else if (response instanceof StorageNodeV2.MultipartData md) {
                    ctx.status(200)
                            .header("Content-Type", "application/json")
                            .json(md.version().chunks());
                    logger.debug("GET partition={} fullKey={} version={} returned multipart chunks in body", partition, fullKey,
                            responseSequenceNumber);

                } else if (response instanceof StorageNodeV2.Tombstone) {
                    ctx.status(200).result("");
                    logger.debug("GET partition={} fullKey={} version={} hit tombstone", partition, fullKey,
                            responseSequenceNumber);
                }
            } else {
                ctx.status(404).result("NOT_FOUND");
                logger.debug("GET partition={} fullKey={} targetVersion={} not found", partition, fullKey, targetVersion);
            }
            }
        } catch (Exception e) {
            logger.error("Error handling GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleHeadObject(Context ctx) {
        try {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String fullKey = bucket + "/" + key;

            Optional<Metadata> metaOpt = db.getItem(fullKey);
            if (metaOpt.isEmpty() || metaOpt.get().versions().isEmpty()) {
                ctx.status(404);
                return;
            }

            FileVersion latest = metaOpt.get().versions().get(metaOpt.get().versions().size() - 1);
            String typeHeader;
            long logicalSize = -1;

            if (latest instanceof RegularFileVersion r) {
                typeHeader = "BLOB";
                logicalSize = r.size();
            } else if (latest instanceof MultipartFileVersion m) {
                typeHeader = "MULTIPART";
                logicalSize = m.size();
            } else {
                typeHeader = "TOMBSTONE";
            }

            ctx.header("X-Koop-Version", String.valueOf(latest.sequenceNumber()));
            ctx.header("X-Koop-Type", typeHeader);
            if (logicalSize >= 0) {
                ctx.header("X-Koop-Size", String.valueOf(logicalSize));
            }
            ctx.status(200);
        } catch (Exception e) {
            logger.error("Error handling HEAD object", e);
            ctx.status(500);
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
                    boolean materialized = storageNode.commit(partition, fullKey, m.requestID(), seqNumber, m.size());
                    if (!materialized) {
                        repairQueue.enqueue(new RepairOperation(fullKey, seqNumber, m.requestID()));
                    }
                    requestId = m.requestID();
                    logger.info("Committed file: {}", fullKey);
                }
                case Message.MultipartCommitMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.multipartCommit(partition, fullKey, seqNumber, m.chunks(), m.size());
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

    private void repairBlob(RepairOperation operation) {
        String blobKey = operation.blobKey();
        long seqOffset = operation.seqOffset();

        logger.info("Starting repair: key={}, seqOffset={}", blobKey, seqOffset);

        if (currentEsConfig == null || currentPsConfig == null) {
            logger.warn("Repair skipped: metadata config not yet available for key={}", blobKey);
            return;
        }

        ErasureRouting routing = new ErasureRouting(currentPsConfig, currentEsConfig);

        var partitionOpt = routing.getPartition(blobKey);
        if (partitionOpt.isEmpty()) {
            logger.error("Repair failed: cannot determine partition for key={}", blobKey);
            return;
        }
        int partition = partitionOpt.getAsInt();

        var esOpt = routing.getErasureSet(partition);
        if (esOpt.isEmpty()) {
            logger.error("Repair failed: no erasure set for partition={} key={}", partition, blobKey);
            return;
        }
        ErasureSetConfiguration.ErasureSet es = esOpt.get();
        List<ErasureSetConfiguration.Machine> machines = es.getMachines();
        int n = es.getN();
        int m = es.getM();

        int myIndex = this.myShardIndex;
        if (myIndex == -1) {
            logger.error("Repair failed: this node {}:{} not found in erasure set {} for key={}",
                    this.ip, app.port(), es.getNumber(), blobKey);
            return;
        }

        InputStream[] shardStreams = new InputStream[n];
        boolean[] present = new boolean[n];

        List<Callable<ShardFetchResult>> fetchTasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (i == myIndex) continue;
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

            if (fetched < m) {
                logger.error("Repair failed: only {}/{} shards fetched for key={}", fetched, m, blobKey);
                return;
            }

            try (InputStream reconstructed = erasureCoder.reconstruct(shardStreams, present, m, n)) {
                byte[] fullData = reconstructed.readAllBytes();

                InputStream[] resharded = erasureCoder.shard(
                        new java.io.ByteArrayInputStream(fullData),
                        fullData.length, m, n);

                byte[] ourShard = resharded[myIndex].readAllBytes();

                storageNode.store(partition, blobKey, operation.requestId(),
                        Channels.newChannel(new java.io.ByteArrayInputStream(ourShard)));

                logger.info("Repair succeeded: key={}, seqOffset={}, shard index={}, {} bytes written",
                        blobKey, seqOffset, myIndex, ourShard.length);
            }
        } catch (Exception e) {
            logger.error("Repair failed for key={}: {}", blobKey, e.getMessage(), e);
        } finally {
            fetchExecutor.shutdownNow();
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

    // ─── Bucket query handlers ────────────────────────────────────────────────

    private void handleHeadBucket(Context ctx) {
        try {
            String bucketName = ctx.pathParam("bucket");
            Optional<Bucket> bucketOpt = storageNode.getBucket(bucketName);

            if (bucketOpt.isPresent()) {
                Bucket bucket = bucketOpt.get();
                // Attach the sequence number so StorageWorker can resolve tie-breakers
                ctx.header("X-Bucket-Version", String.valueOf(bucket.sequenceNumber()));

                if (bucket.deleted()) {
                    ctx.status(410); // Explicitly tombstoned
                } else {
                    ctx.status(200); // Active
                }
            } else {
                ctx.status(404); // Never existed
            }
        } catch (Exception e) {
            logger.error("Error handling HEAD /bucket", e);
            ctx.status(500);
        }
    }

    private void handleListObjects(Context ctx) {
        try {
            String bucket = ctx.pathParam("bucket");
            String prefix = ctx.queryParam("prefix");
            String maxKeysParam = ctx.queryParam("maxKeys");
            int maxKeys = (maxKeysParam != null) ? Integer.parseInt(maxKeysParam) : 1000;

            if (maxKeys < 0) {
                ctx.status(400).result("maxKeys must be non-negative");
                return;
            }

            if (!storageNode.bucketExists(bucket)) {
                ctx.status(404);
                return;
            }

            String scanPrefix = bucket + "/";
            if (prefix != null && !prefix.isEmpty()) {
                scanPrefix = bucket + "/" + prefix;
            }

            List<Map<String, Object>> items;
            try (var stream = storageNode.listItemsInBucket(scanPrefix)) {
                items = stream
                        .filter(m -> {
                            var versions = m.versions();
                            if (versions == null || versions.isEmpty()) return false;

                            // Filter out tombstones
                            if (versions.getLast() instanceof TombstoneFileVersion) return false;

                            // Filter out internal multipart chunk objects.
                            // MultipartUploadSession prefixes chunk keys with "__mpu__:"
                            if (m.key().contains("/__mpu__:")) {
                                return false;
                            }

                            return true;
                        })
                        .limit(maxKeys)
                        .map(m -> Map.<String, Object>of(
                                "key", m.key(),
                                "size", m.versions().getLast().size(),
                                "lastModified", "1970-01-01T00:00:00.000Z"
                        ))
                        .toList();
            }

            ctx.status(200).json(items);
        } catch (NumberFormatException e) {
            ctx.status(400).result("Invalid maxKeys parameter");
        } catch (Exception e) {
            logger.error("Error handling GET /bucket", e);
            ctx.status(500);
        }
    }

    public void start() {
        repairWorkerPool.start();
        activeReads.start();
        gossipService.start();
        gcWorker.start();
        blobDeletionWorker.start();

        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.put("/store/{partition}/{bucket}/<key>", this::handlePut);
            config.routes.get("/store/{partition}/{bucket}/<key>", this::handleGet);
            config.routes.head("/store/{partition}/{bucket}/<key>", this::handleHeadObject);

            config.routes.head("/bucket/{bucket}", this::handleHeadBucket);
            config.routes.get("/bucket/{bucket}", this::handleListObjects);

            config.routes.get("/health", ctx -> ctx.status(200).result("OK"));
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
        activeReads.stop();
        gossipService.stop();
        gcWorker.stop();
        blobDeletionWorker.stop();

        partitionExecutors.values().forEach(ExecutorService::shutdownNow);
        partitionExecutors.clear();

        erasureCoder.shutdown();

        logger.info("StorageNodeServerV2 stopped");
    }

    public int port() {
        return app != null ? app.port() : port;
    }

    int repairPendingCount() {
        return repairWorkerPool.pendingCount();
    }

    /** Test hook: expose the active read tracker. */
    public ActiveReadTracker activeReadTracker() {
        return activeReads;
    }

    /** Test hook: expose the watermark store. */
    public PartitionWatermarks partitionWatermarks() {
        return watermarks;
    }

    /** Test hook: expose the gossip service. */
    public GossipService gossipService() {
        return gossipService;
    }

    /** Test hook: expose the GC worker. */
    public GarbageCollectorWorker gcWorker() {
        return gcWorker;
    }

    /** Test hook: expose the disk-reconciliation worker. */
    public BlobDeletionWorker blobDeletionWorker() {
        return blobDeletionWorker;
    }
}