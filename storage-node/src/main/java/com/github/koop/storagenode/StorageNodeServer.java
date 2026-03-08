package com.github.koop.storagenode;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.javalin.Javalin;
import io.javalin.http.Context;

/**
 * Storage Node HTTP server powered by Javalin + virtual threads.
 *
 * Endpoints:
 *   PUT    /store/{partition}/{key}?requestId=...  — store a shard
 *   GET    /store/{partition}/{key}                — retrieve a shard
 *   DELETE /store/{partition}/{key}                — delete a shard
 *   GET    /health                                 — health check
 */
public class StorageNodeServer {

    private final int port;
    private final StorageNode storageNode;
    private Javalin app;

    private static final Logger logger = LogManager.getLogger(StorageNodeServer.class);

    public StorageNodeServer(int port, Path dir) {
        this.port = port;
        this.storageNode = new StorageNode(dir);
    }

    public static void main(String[] args) {
        String envPort = System.getenv("PORT");
        String envDir  = System.getenv("STORAGE_DIR");

        int port      = (envPort != null) ? Integer.parseInt(envPort) : 8080;
        Path storagePath = Path.of((envDir != null) ? envDir : "./storage");

        logger.info("Starting StorageNodeServer with port={} and storagePath={}", port, storagePath);

        try {
            java.nio.file.Files.createDirectories(storagePath);
        } catch (IOException e) {
            logger.error("Failed to create storage directory: {}", storagePath);
            System.exit(1);
        }

        StorageNodeServer server = new StorageNodeServer(port, storagePath);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server...");
            server.stop();
        }));

        server.start();
    }

    // --- Handlers ---

    private void handlePut(Context ctx) {
        try {
            int partition    = Integer.parseInt(ctx.pathParam("partition"));
            String key       = ctx.pathParam("key");
            String requestId = ctx.queryParam("requestId");
            long length      = ctx.contentLength();

            storageNode.store(partition, requestId, key,
                    Channels.newChannel(ctx.bodyInputStream()), length);

            ctx.status(200).result("OK");
            logger.debug("PUT partition={} key={} requestId={} length={}", partition, key, requestId, length);
        } catch (Exception e) {
            logger.error("Error handling PUT", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleGet(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key    = ctx.pathParam("key");

            var dataOpt = storageNode.retrieve(partition, key);
            if (dataOpt.isPresent()) {
                try (var fc = dataOpt.get()) {
                    byte[] data = new byte[(int) fc.size()];
                    var buf = java.nio.ByteBuffer.wrap(data);
                    while (buf.hasRemaining()) fc.read(buf);

                    ctx.status(200)
                       .header("Content-Type", "application/octet-stream")
                       .result(data);
                    logger.debug("GET partition={} key={} found={} bytes", partition, key, data.length);
                }
            } else {
                ctx.status(404).result("");
                logger.debug("GET partition={} key={} not found", partition, key);
            }
        } catch (Exception e) {
            logger.error("Error handling GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleDelete(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key    = ctx.pathParam("key");

            boolean result = storageNode.delete(partition, key);
            if (result) {
                ctx.status(200).result("OK");
            } else {
                ctx.status(404).result("NOT_FOUND");
            }
            logger.debug("DELETE partition={} key={} result={}", partition, key, result);
        } catch (Exception e) {
            logger.error("Error handling DELETE", e);
            ctx.status(500).result("ERROR");
        }
    }

    // --- Server Lifecycle ---

    public void start() {
        app = Javalin.create(config -> {
            config.useVirtualThreads = true;
            config.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L; // 100 MB — shards can be large
        });

        app.get("/health", ctx -> ctx.result("OK"));
        app.put("/store/{partition}/{key}", this::handlePut);
        app.get("/store/{partition}/{key}", this::handleGet);
        app.delete("/store/{partition}/{key}", this::handleDelete);

        app.start(port);
        logger.info("StorageNodeServer started on port {}", app.port());
    }

    public void stop() {
        if (app != null) {
            app.stop();
            logger.info("StorageNodeServer stopped");
        }
    }

    /** Returns the actual port the server is listening on (useful when started with port 0). */
    public int port() {
        return app != null ? app.port() : port;
    }
}
