package com.github.koop.storagenode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.messages.Message;
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

    /** Current lifecycle state of this node. */
    private volatile NodeState state = NodeState.INITIALIZING;

    private Javalin app;
    private ErasureSetConfiguration currentEsConfig;
    private PartitionSpreadConfiguration currentPsConfig;

    private final Set<Integer> subscribedPartitions = new HashSet<>();
    private final Map<Integer, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Logger logger = LogManager.getLogger(StorageNodeServerV2.class);

    public StorageNodeServerV2(int port, String ip, Database db, Path dir, MetadataClient metadataClient,
            PubSubClient pubSubClient) {
        this(port, ip, db, dir, metadataClient, pubSubClient, RepairWorkerPool.DEFAULT_CONCURRENCY);
    }

    public StorageNodeServerV2(int port, String ip, Database db, Path dir, MetadataClient metadataClient,
            PubSubClient pubSubClient, int repairConcurrency) {
        this.port = port;
        this.ip = ip;
        this.repairWorkerPool = new RepairWorkerPool(repairConcurrency);
        this.storageNode = new StorageNodeV2(db, dir, this.repairWorkerPool);
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

        for (ErasureSetConfiguration.ErasureSet es : currentEsConfig.getErasureSets()) {
            for (ErasureSetConfiguration.Machine machine : es.getMachines()) {
                if (machine.getIp().equals(this.ip) && machine.getPort() == this.port) {
                    myErasureSetId = es.getNumber();
                    break;
                }
            }
            if (myErasureSetId != -1)
                break;
        }

        Set<Integer> targetPartitions = new HashSet<>();

        if (myErasureSetId == -1) {
            logger.warn("Node {}:{} not found in any Erasure Set. Dropping all partition subscriptions.", this.ip,
                    this.port);
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

        String consumerGroupId = this.ip + ":" + this.port;

        for (Integer partition : toAdd) {
            String topic = CommitTopics.forPartition(partition);
            logger.info("Node assigned to partition {}. Subscribing to topic: {} with consumerGroupId: {}",
                    partition, topic, consumerGroupId);

            ExecutorService partitionExecutor = Executors.newSingleThreadExecutor(
                    Thread.ofVirtual().name("partition-" + partition + "-").factory());
            partitionExecutors.put(partition, partitionExecutor);

            pubSubClient.sub(topic, consumerGroupId, (incomingTopic, offset, messageBytes) -> {
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
                    storageNode.commit(partition, fullKey, m.requestID(), seqNumber);
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
        state = NodeState.REPAIRING;
        logger.info("Node entering REPAIRING state");

        // Start the repair worker pool
        repairWorkerPool.start();

        if (metadataClient != null) {
            this.currentEsConfig = metadataClient.get(ErasureSetConfiguration.class);
            this.currentPsConfig = metadataClient.get(PartitionSpreadConfiguration.class);
            updateSubscriptions();

            // Perform startup repair: consume and compact backlog for all subscribed partitions
            performStartupRepair();

            metadataClient.listen(ErasureSetConfiguration.class, (prev, current) -> {
                this.currentEsConfig = current;
                updateSubscriptions();
            });

            metadataClient.listen(PartitionSpreadConfiguration.class, (prev, current) -> {
                this.currentPsConfig = current;
                updateSubscriptions();
            });
        }

        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.before(this::gateTraffic);
            config.routes.put("/store/{partition}/{bucket}/<key>", this::handlePut);
            config.routes.get("/store/{partition}/{bucket}/<key>", this::handleGet);
        });

        app.start(port);

        // Transition to ACTIVE — node is ready to serve traffic
        state = NodeState.ACTIVE;
        logger.info("StorageNodeServerV2 started on port {} — state: ACTIVE", app.port());
    }

    /**
     * Blocks HTTP traffic while the node is not in ACTIVE state.
     * Returns 503 Service Unavailable with the current node state.
     */
    private void gateTraffic(Context ctx) {
        if (state != NodeState.ACTIVE) {
            ctx.status(503).result("Node is not ready: " + state);
            ctx.skipRemainingHandlers();
        }
    }

    /**
     * Consumes all backlog messages for subscribed partitions, compacts them
     * (retaining only the latest operation per key), and enqueues repair
     * operations for keys that need data reconstruction.
     */
    private void performStartupRepair() {
        int totalBacklog = 0;
        int totalRepairs = 0;

        for (Integer partition : subscribedPartitions) {
            String topic = CommitTopics.forPartition(partition);
            List<byte[]> backlog = pubSubClient.pollBacklog(topic);

            if (backlog.isEmpty()) {
                logger.debug("No backlog for partition {} (topic: {})", partition, topic);
                continue;
            }

            totalBacklog += backlog.size();
            logger.info("Partition {} has {} backlog messages to compact", partition, backlog.size());

            // Compact: retain only the latest message per unique key (last-writer-wins)
            Map<String, Message> compacted = new LinkedHashMap<>();
            for (byte[] msgBytes : backlog) {
                try {
                    Message message = Message.deserializeMessage(msgBytes);
                    String key = extractKeyFromMessage(message);
                    if (key != null) {
                        compacted.put(key, message);
                    }
                } catch (Exception e) {
                    logger.error("Failed to deserialize backlog message during compaction", e);
                }
            }

            // Enqueue repair operations for keys whose final state requires data
            for (Map.Entry<String, Message> entry : compacted.entrySet()) {
                Message finalMessage = entry.getValue();
                // Skip deletes and bucket operations — only data writes need repair
                if (finalMessage instanceof Message.FileCommitMessage
                        || finalMessage instanceof Message.MultipartCommitMessage) {
                    repairWorkerPool.enqueue(
                            new RepairOperation(entry.getKey(), RepairOperation.RepairReason.STARTUP_CATCHUP));
                    totalRepairs++;
                }
            }
        }

        logger.info("Startup repair complete: processed {} backlog messages, enqueued {} repair operations",
                totalBacklog, totalRepairs);
    }

    /**
     * Extracts the full object key from a message for compaction purposes.
     * Returns null for messages that don't map to an object key (e.g. bucket operations).
     */
    private String extractKeyFromMessage(Message message) {
        return switch (message) {
            case Message.FileCommitMessage m -> buildKey(m.bucket(), m.key());
            case Message.MultipartCommitMessage m -> buildKey(m.bucket(), m.key());
            case Message.DeleteMessage m -> buildKey(m.bucket(), m.key());
            case Message.CreateBucketMessage m -> "bucket:" + m.bucket();
            case Message.DeleteBucketMessage m -> "bucket:" + m.bucket();
        };
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }

        repairWorkerPool.shutdown();

        partitionExecutors.values().forEach(ExecutorService::shutdownNow);
        partitionExecutors.clear();

        state = NodeState.INITIALIZING;
        logger.info("StorageNodeServerV2 stopped");
    }

    /**
     * Returns the current lifecycle state of this node.
     */
    public NodeState getState() {
        return state;
    }

    public int port() {
        return app != null ? app.port() : port;
    }
}