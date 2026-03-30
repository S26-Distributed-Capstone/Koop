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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.koop.common.messages.Message;
import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;

import io.javalin.Javalin;
import io.javalin.http.Context;

/**
 * Storage Node HTTP server powered by Javalin + virtual threads.
 * Integrates MetadataClient and PubSubClient for dynamic partition assignment
 * and sequencer log tailing.
 */
public class StorageNodeServerV2 {

    private final int port;
    private final String ip;
    private final StorageNodeV2 storageNode;
    private final MetadataClient metadataClient;
    private final PubSubClient pubSubClient;

    private Javalin app;
    private ErasureSetConfiguration currentEsConfig;
    private PartitionSpreadConfiguration currentPsConfig;

    // Tracks active partition subscriptions and their dedicated executors
    private final Set<Integer> subscribedPartitions = new HashSet<>();
    private final ExecutorService executorService;

    // HTTP Client for sending asynchronous acknowledgments
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Logger logger = LogManager.getLogger(StorageNodeServerV2.class);

    public StorageNodeServerV2(int port, String ip, Database db, Path dir, MetadataClient metadataClient,
            PubSubClient pubSubClient) {
        this.port = port;
        this.ip = ip;
        this.storageNode = new StorageNodeV2(db, dir);
        this.metadataClient = metadataClient;
        this.pubSubClient = pubSubClient;
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
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

        // Injected dependencies
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

    // --- Dynamic Configuration & Subscriptions ---

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
        }

        Set<Integer> toAdd = new HashSet<>(targetPartitions);
        toAdd.removeAll(subscribedPartitions);

        for (Integer partition : toAdd) {
            String topic = "partition-" + partition;
            logger.info("Node assigned to partition {}. Subscribing to topic: {}", partition, topic);

            pubSubClient.sub(topic, (incomingTopic, offset, messageBytes) -> {
                executorService.submit(() -> {
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

    // --- HTTP Handlers ---

    private void handlePut(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key = ctx.pathParam("key");
            String requestId = ctx.queryParam("requestId");
            long length = ctx.req().getContentLengthLong();

            if (requestId == null || requestId.isBlank()) {
                ctx.status(400).result("Missing requestId parameter");
                return;
            }

            storageNode.store(partition, requestId, Channels.newChannel(ctx.bodyInputStream()), length);

            ctx.status(200).result("OK");
            logger.debug("PUT (Uncommitted) partition={} key={} requestId={}", partition, key, requestId);
        } catch (Exception e) {
            logger.error("Error handling PUT", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleGet(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key = ctx.pathParam("key");

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

            Optional<StorageNodeV2.GetObjectResponse> dataOpt = storageNode.retrieve(key, targetVersion);

            if (dataOpt.isPresent()) {
                StorageNodeV2.GetObjectResponse response = dataOpt.get();

                long responseSequenceNumber = switch (response) {
                    case StorageNodeV2.FileObject fo -> fo.version().sequenceNumber();
                    case StorageNodeV2.MultipartData md -> md.version().sequenceNumber();
                    case StorageNodeV2.Tombstone t -> t.version().sequenceNumber();
                };

                ctx.header("X-Object-Version", String.valueOf(responseSequenceNumber));

                if (response instanceof StorageNodeV2.FileObject fo) {
                    var fc = fo.data();
                    ctx.status(200)
                            .header("Content-Type", "application/octet-stream")
                            .header("Content-Length", String.valueOf(fc.size()));

                    var outputChannel = Channels.newChannel(ctx.res().getOutputStream());
                    long size = fc.size();
                    long position = 0L;
                    while (position < size) {
                        long transferred = fc.transferTo(position, size - position, outputChannel);
                        if (transferred <= 0) {
                            logger.warn(
                                    "Zero-byte transfer when streaming file for partition={} key={} at position={} of {} bytes",
                                    partition, key, position, size);
                            break;
                        }
                        position += transferred;
                    }
                    fo.close();
                    logger.debug("GET partition={} key={} version={} streamed {} bytes", partition, key,
                            responseSequenceNumber, size);

                } else if (response instanceof StorageNodeV2.MultipartData md) {
                    ctx.status(200)
                            .header("Content-Type", "application/json")
                            .json(md.version().chunks());
                    logger.debug("GET partition={} key={} version={} returned multipart chunk list", partition, key,
                            responseSequenceNumber);

                } else if (response instanceof StorageNodeV2.Tombstone) {
                    ctx.status(404).result("NOT_FOUND (Tombstone)");
                    logger.debug("GET partition={} key={} version={} hit tombstone", partition, key,
                            responseSequenceNumber);
                }
            } else {
                ctx.status(404).result("NOT_FOUND");
                logger.debug("GET partition={} key={} targetVersion={} not found", partition, key, targetVersion);
            }
        } catch (Exception e) {
            logger.error("Error handling GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    // --- Pub/Sub Mutator Flow ---

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

            // Fire and forget acknowledgment
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
            String url = callbackAddress.endsWith("/") ? callbackAddress + "ack" : callbackAddress + "/ack";
            String jsonPayload = String.format("{\"requestId\":\"%s\"}", requestId);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
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

    // --- Server Lifecycle ---

    public void start() {
        if (metadataClient != null) {
            metadataClient.listen(ErasureSetConfiguration.class, (prev, current) -> {
                this.currentEsConfig = current;
                updateSubscriptions();
            });

            metadataClient.listen(PartitionSpreadConfiguration.class, (prev, current) -> {
                this.currentPsConfig = current;
                updateSubscriptions();
            });

            metadataClient.start();
        }

        if (pubSubClient != null) {
            pubSubClient.start();
        }

        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;
            config.routes.put("/store/{partition}/{key}", this::handlePut);
            config.routes.get("/store/{partition}/{key}", this::handleGet);
        });

        app.start(port);
        logger.info("StorageNodeServerV2 started on port {}", app.port());
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }

        try {
            if (metadataClient != null) {
                metadataClient.close();
            }
            if (pubSubClient != null) {
                pubSubClient.close();
            }
        } catch (Exception e) {
            logger.error("Error shutting down clients", e);
        }
        logger.info("StorageNodeServerV2 stopped");
    }

    public int port() {
        return app != null ? app.port() : port;
    }
}