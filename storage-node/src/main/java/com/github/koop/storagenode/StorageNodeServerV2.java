package com.github.koop.storagenode;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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
    private final Set<Integer> subscribedPartitions = new HashSet<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LogManager.getLogger(StorageNodeServerV2.class);

    public StorageNodeServerV2(int port, String ip, Database db, Path dir, MetadataClient metadataClient, PubSubClient pubSubClient) {
        this.port = port;
        this.ip = ip;
        this.storageNode = new StorageNodeV2(db, dir);
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

        // Injected dependencies
        Database db = null; // e.g., new RocksDbStorageStrategy(...)
        MetadataClient metadataClient = null; // Setup with appropriate Fetcher
        PubSubClient pubSubClient = null; // Setup with appropriate PubSub implementation

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

        // 1. Determine which erasure set this node belongs to
        for (ErasureSetConfiguration.ErasureSet es : currentEsConfig.getErasureSets()) {
            for (ErasureSetConfiguration.Machine machine : es.getMachines()) {
                if (machine.getIp().equals(this.ip) && machine.getPort() == this.port) {
                    myErasureSetId = es.getNumber();
                    break;
                }
            }
            if (myErasureSetId != -1) break;
        }

        if (myErasureSetId == -1) {
            logger.warn("Node {}:{} not found in any Erasure Set.", this.ip, this.port);
            return;
        }

        // 2. Discover partitions mapped to this erasure set and subscribe
        for (PartitionSpreadConfiguration.PartitionSpread spread : currentPsConfig.getPartitionSpread()) {
            if (spread.getErasureSet() == myErasureSetId) {
                for (Integer partition : spread.getPartitions()) {
                    if (subscribedPartitions.add(partition)) {
                        String topic = "partition-" + partition;
                        logger.info("Node assigned to partition {}. Subscribing to topic: {}", partition, topic);
                        
                        pubSubClient.sub(topic, (incomingTopic, offset, messageBytes) -> {
                            try {
                                Message message = objectMapper.readValue(messageBytes, Message.class);
                                processSequencerMessage(partition, message, offset);
                            } catch (Exception e) {
                                logger.error("Failed to deserialize or process message on topic {} at offset {}", incomingTopic, offset, e);
                            }
                        });
                    }
                }
            }
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
            
            Optional<StorageNodeV2.GetObjectResponse> dataOpt = storageNode.retrieve(key);
            
            if (dataOpt.isPresent()) {
                StorageNodeV2.GetObjectResponse response = dataOpt.get();
                
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
                            logger.warn("Zero-byte transfer when streaming file for partition={} key={} at position={} of {} bytes",
                                    partition, key, position, size);
                            break;
                        }
                        position += transferred;
                    }
                    fo.close();
                    logger.debug("GET partition={} key={} streamed {} bytes", partition, key, size);
                    
                } else if (response instanceof StorageNodeV2.MultipartData md) {
                    ctx.status(200)
                            .header("Content-Type", "application/json")
                            .json(md.version().chunks());
                    logger.debug("GET partition={} key={} returned multipart chunk list", partition, key);
                    
                } else if (response instanceof StorageNodeV2.Tombstone) {
                    ctx.status(404).result("NOT_FOUND (Tombstone)");
                    logger.debug("GET partition={} key={} hit tombstone", partition, key);
                }
            } else {
                ctx.status(404).result("NOT_FOUND");
                logger.debug("GET partition={} key={} not found", partition, key);
            }
        } catch (Exception e) {
            logger.error("Error handling GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    // --- Pub/Sub Mutator Flow ---

    /**
     * Processes mutations routed from the PubSubClient listener.
     */
    public void processSequencerMessage(int partition, Message message, long seqNumber) {
        try {
            switch (message) {
                case Message.FileCommitMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.commit(partition, fullKey, m.requestID(), seqNumber);
                    logger.info("Committed file: {}", fullKey);
                }
                case Message.MultipartCommitMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.multipartCommit(partition, fullKey, seqNumber, m.chunks());
                    logger.info("Committed multipart file: {}", fullKey);
                }
                case Message.DeleteMessage m -> {
                    String fullKey = buildKey(m.bucket(), m.key());
                    storageNode.delete(partition, fullKey, seqNumber);
                    logger.info("Deleted file (Tombstone): {}", fullKey);
                }
                case Message.CreateBucketMessage m -> {
                    storageNode.createBucket(partition, m.bucket(), seqNumber);
                    logger.info("Created bucket: {}", m.bucket());
                }
                case Message.DeleteBucketMessage m -> {
                    storageNode.deleteBucket(partition, m.bucket(), seqNumber);
                    logger.info("Deleted bucket: {}", m.bucket());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to process sequencer message: " + message, e);
        }
    }

    private String buildKey(String bucket, String key) {
        if (bucket == null || bucket.isEmpty()) return key;
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
            config.routes.get("/health", ctx -> ctx.result("OK"));
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