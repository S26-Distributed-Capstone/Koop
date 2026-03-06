package com.github.koop.queryprocessor.gateway;

import io.javalin.Javalin;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.logging.Logger;
import java.util.List;
import java.util.logging.Level;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.StorageWorker;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    /**
     * Creates and configures the Javalin app with all S3-compatible routes.
     * Accepts a StorageService so tests can inject a mock.
     */
    public static Javalin createApp(StorageService storage) {
        var app = Javalin.create(config -> {
            config.useVirtualThreads = true;
        });

        app.get("/health", ctx -> ctx.result("API Gateway is healthy!"));

        // GET /{bucket}/{key}
        app.get("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String resourcePath = "/" + bucket + "/" + key;

            try {
                InputStream data = storage.getObject(bucket, key);
                if (data != null) {
                    ctx.status(200);
                    ctx.header("Content-Type", "application/octet-stream");
                    ctx.header("ETag", "\"dummy-etag-12345\"");
                    ctx.result(data);
                } else {
                    ctx.status(404);
                    ctx.header("Content-Type", "application/xml");
                    ctx.result(buildS3ErrorXml("NoSuchKey", "The specified key does not exist.", resourcePath));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format("Error in GET /%s/%s", bucket, key), e);
                ctx.status(500);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
            }
        });

        // PUT /{bucket}/{key}
        app.put("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            long contentLength = ctx.contentLength();
            String resourcePath = "/" + bucket + "/" + key;

            try {
                InputStream is = ctx.bodyInputStream();
                storage.putObject(bucket, key, is, contentLength);
                ctx.status(200);
                ctx.header("ETag", "\"dummy-etag-12345\"");
                ctx.result("");
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format("Error in PUT /%s/%s", bucket, key), e);
                ctx.status(500);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
            }
        });

        // DELETE /{bucket}/{key}
        app.delete("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String resourcePath = "/" + bucket + "/" + key;

            try {
                storage.deleteObject(bucket, key);
                ctx.status(204);
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format("Error in DELETE /%s/%s", bucket, key), e);
                ctx.status(500);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
            }
        });

        return app;
    }

    public static void main(String[] args) {
        // TODO: Replace with MetadataClient once PR #56 (better-metadata-client) is merged.
        // At that point: new MetadataClient(new EtcdFetcher(etcdEndpoints)) will supply
        // the three replica sets dynamically. For now, read node addresses from env vars
        // that docker-compose injects via service DNS (storage-node-1 through storage-node-6).
        StorageWorker storageWorker = buildStorageWorkerFromEnv();
        StorageService storage = new StorageWorkerService(storageWorker);
        createApp(storage).start(8080);
    }

    private static StorageWorker buildStorageWorkerFromEnv() {
        int port = 8080;
        // Docker Compose service names use underscores (required for Testcontainers
        // ComposeContainer compatibility), but Docker's internal DNS only resolves
        // hostnames with hyphens — underscores are invalid per RFC 952 and Docker's
        // embedded DNS silently fails to resolve them. Docker auto-aliases
        // underscore service names to their hyphenated equivalent as the container
        // hostname, so we use hyphens here for DNS resolution.
        List<InetSocketAddress> nodes = List.of(
            new InetSocketAddress("storage-node-1", port),
            new InetSocketAddress("storage-node-2", port),
            new InetSocketAddress("storage-node-3", port),
            new InetSocketAddress("storage-node-4", port),
            new InetSocketAddress("storage-node-5", port),
            new InetSocketAddress("storage-node-6", port),
            new InetSocketAddress("storage-node-7", port),
            new InetSocketAddress("storage-node-8", port),
            new InetSocketAddress("storage-node-9", port)
        );
        return new StorageWorker(nodes, nodes, nodes);
    }

    // Helper method to generate S3-compliant XML error responses
    static String buildS3ErrorXml(String code, String message, String resource) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<Error>\n" +
               "  <Code>" + code + "</Code>\n" +
               "  <Message>" + message + "</Message>\n" +
               "  <Resource>" + resource + "</Resource>\n" +
               "  <RequestId>" + java.util.UUID.randomUUID().toString() + "</RequestId>\n" +
               "</Error>";
    }
}