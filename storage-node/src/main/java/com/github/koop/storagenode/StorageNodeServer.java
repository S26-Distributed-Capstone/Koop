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
 * PUT /store/{partition}/{key}?requestId=... — store a shard
 * GET /store/{partition}/{key} — retrieve a shard
 * DELETE /store/{partition}/{key} — delete a shard
 * GET /health — health check
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
        String envDir = System.getenv("STORAGE_DIR");

        int port = (envPort != null) ? Integer.parseInt(envPort) : 8080;
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
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key = ctx.pathParam("key");
            String requestId = ctx.queryParam("requestId");

            storageNode.store(partition, requestId, key,
                    Channels.newChannel(ctx.bodyInputStream()));

            ctx.status(200).result("OK");
            logger.debug("PUT partition={} key={} requestId={}", partition, key, requestId);
        } catch (Exception e) {
            logger.error("Error handling PUT", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleGet(Context ctx) {
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String key = ctx.pathParam("key");

            var dataOpt = storageNode.retrieve(partition, key);
            if (dataOpt.isPresent()) {
                var fc = dataOpt.get();
                ctx.status(200)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", String.valueOf(fc.size()));
                        //.result(Channels.newInputStream(fc));
                // should use zero-copy transfer to avoid unnecessary buffering in memory
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
                logger.debug("GET partition={} key={} streaming {} bytes", partition, key, fc.size());
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
            String key = ctx.pathParam("key");

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
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L; // 100 MB — shards can be large

            config.routes.get("/health", ctx -> ctx.result("OK"));
            config.routes.put("/store/{partition}/{key}", this::handlePut);
            config.routes.get("/store/{partition}/{key}", this::handleGet);
            config.routes.delete("/store/{partition}/{key}", this::handleDelete);
        });

        app.start(port);
        logger.info("StorageNodeServer started on port {}", app.port());
    }

    public void stop() {
        if (app != null) {
            app.stop();
            logger.info("StorageNodeServer stopped");
        }
    }

    /**
     * Returns the actual port the server is listening on (useful when started with
     * port 0).
     */
    public int port() {
        return app != null ? app.port() : port;
    }
}
