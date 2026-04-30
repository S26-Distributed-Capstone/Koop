package com.github.koop.queryprocessor.processor;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fake storage node HTTP server for testing.
 * Mimics the real StorageNodeServer Javalin endpoints.
 */
public class FakeStorageNodeServer implements Closeable {

    private final Javalin app;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private final Set<String> buckets = ConcurrentHashMap.newKeySet();
    private volatile boolean enabled = true;

    private final Logger logger = LogManager.getLogger(FakeStorageNodeServer.class);

    public FakeStorageNodeServer() {
        app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.http.maxRequestSize = 100_000_000L; // 100 MB — tests use large payloads

            config.routes.put("/store/{partition}/<key>", this::handlePut);
            config.routes.get("/store/{partition}/<key>", this::handleGet);
            config.routes.delete("/store/{partition}/<key>", this::handleDelete);

            config.routes.head("/bucket/{bucket}", this::handleHeadBucket);
            config.routes.get("/bucket/{bucket}", this::handleListObjects);
        });

        app.start(0); // OS picks a free port
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
            String key    = ctx.pathParam("key");
            byte[] data   = ctx.bodyAsBytes();

            store.put(mapKey(partition, key), data);
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
            String key    = ctx.pathParam("key");

            byte[] data = store.get(mapKey(partition, key));
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
            String key    = ctx.pathParam("key");

            store.remove(mapKey(partition, key));
            ctx.status(200).result("OK");
        } catch (Exception e) {
            logger.error("Error in fake DELETE", e);
            ctx.status(500).result("ERROR");
        }
    }

    private void handleHeadBucket(Context ctx) {
        if (!enabled) {
            ctx.status(503).result("NODE_DISABLED");
            return;
        }
        String bucket = ctx.pathParam("bucket");
        ctx.status(buckets.contains(bucket) ? 200 : 404);
    }

    private void handleListObjects(Context ctx) {
        if (!enabled) {
            ctx.status(503).result("NODE_DISABLED");
            return;
        }
        try {
            String bucket = ctx.pathParam("bucket");
            if (!buckets.contains(bucket)) {
                ctx.status(404);
                return;
            }
            String prefix = ctx.queryParam("prefix");
            String maxKeysParam = ctx.queryParam("maxKeys");
            int maxKeys = (maxKeysParam != null) ? Integer.parseInt(maxKeysParam) : 1000;

            String scanPrefix = bucket + "/" + (prefix == null ? "" : prefix);

            String body = store.keySet().stream()
                    .map(k -> k.substring(k.indexOf('|') + 1))
                    .filter(k -> k.startsWith(scanPrefix))
                    .distinct()
                    .sorted()
                    .limit(maxKeys)
                    .map(k -> "{\"key\":\"" + k.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}")
                    .collect(Collectors.joining(",", "[", "]"));

            ctx.status(200).header("Content-Type", "application/json").result(body);
        } catch (NumberFormatException e) {
            ctx.status(400).result("Invalid maxKeys parameter");
        } catch (Exception e) {
            logger.error("Error in fake list objects", e);
            ctx.status(500).result("ERROR");
        }
    }

    void addBucket(String bucket) {
        buckets.add(bucket);
    }

    void removeBucket(String bucket) {
        buckets.remove(bucket);
    }

    private static String mapKey(int partition, String key) {
        return partition + "|" + key;
    }

    /**
     * Directly removes a stored shard from the in-memory store.
     * Called by {@link AckingFakeStorageNodeServer} when it receives a
     * {@link com.github.koop.common.messages.Message.DeleteMessage} from the
     * pub/sub bus, mirroring what the real SN does after writing its tombstone.
     */
    void deleteData(int partition, String storageKey) {
        store.remove(mapKey(partition, storageKey));
    }

    @Override
    public void close() throws IOException {
        app.stop();
    }
}