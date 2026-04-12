package com.github.koop.queryprocessor.processor;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FakeStorageNodeServer implements Closeable {

    private final Javalin app;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private volatile boolean enabled = true;

    private final Logger logger = LogManager.getLogger(FakeStorageNodeServer.class);

    public FakeStorageNodeServer() {
        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L;

            config.routes.put("/store/{partition}/{bucket}/<key>", this::handlePut);
            config.routes.get("/store/{partition}/{bucket}/<key>", this::handleGet);
            config.routes.delete("/store/{partition}/{bucket}/<key>", this::handleDelete);
        });

        app.start(0);
    }

    public InetSocketAddress address() {
        return new InetSocketAddress("127.0.0.1", app.port());
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void handlePut(Context ctx) {
        if (!enabled) {
            ctx.status(503).result("NODE_DISABLED");
            return;
        }
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String bucket = ctx.pathParam("bucket");
            String key    = ctx.pathParam("key"); 
            String fullKey = bucket + "/" + key;
            byte[] data   = ctx.bodyAsBytes();

            store.put(mapKey(partition, fullKey), data);
            ctx.status(200).result("OK");
        } catch (Exception e) {
            logger.error("Error in fake PUT", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleGet(Context ctx) {
        if (!enabled) {
            ctx.status(503).result("NODE_DISABLED");
            return;
        }
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String bucket = ctx.pathParam("bucket");
            String key    = ctx.pathParam("key");
            String fullKey = bucket + "/" + key;

            byte[] data = store.get(mapKey(partition, fullKey));
            if (data != null) {
                ctx.status(200)
                   .header("Content-Type", "application/octet-stream")
                   .result(data);
            } else {
                ctx.status(404).result("");
            }
        } catch (Exception e) {
            logger.error("Error in fake GET", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleDelete(Context ctx) {
        if (!enabled) {
            ctx.status(503).result("NODE_DISABLED");
            return;
        }
        try {
            int partition = Integer.parseInt(ctx.pathParam("partition"));
            String bucket = ctx.pathParam("bucket");
            String key    = ctx.pathParam("key");
            String fullKey = bucket + "/" + key;

            store.remove(mapKey(partition, fullKey));
            ctx.status(200).result("OK");
        } catch (Exception e) {
            logger.error("Error in fake DELETE", e);
            ctx.status(500).result("ERROR");
        }
    }

    private static String mapKey(int partition, String fullKey) {
        return partition + "|" + fullKey;
    }

    @Override
    public void close() throws IOException {
        app.stop();
    }
}