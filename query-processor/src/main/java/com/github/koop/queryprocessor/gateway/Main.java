package com.github.koop.queryprocessor.gateway;

import io.javalin.Javalin;
import java.io.InputStream;
import java.util.logging.Logger;
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
            config.concurrency.useVirtualThreads = true;
            config.http.maxRequestSize = 100_000_000L; // 100 MB
            config.routes.get("/health", Main::healthHandler);
            config.routes.get("/{bucket}/{key}", ctx -> getObjectHandler(ctx, storage));
            config.routes.put("/{bucket}/{key}", ctx -> putObjectHandler(ctx, storage));
            config.routes.delete("/{bucket}/{key}", ctx -> deleteObjectHandler(ctx, storage));
        });

        return app;
    }

    private static void healthHandler(io.javalin.http.Context ctx) {
        ctx.result("API Gateway is healthy!");
    }

    private static void getObjectHandler(io.javalin.http.Context ctx, StorageService storage) {
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
    }

    private static void putObjectHandler(io.javalin.http.Context ctx, StorageService storage) {
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
    }

    private static void deleteObjectHandler(io.javalin.http.Context ctx, StorageService storage) {
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
    }

    public static void main(String[] args) {
        StorageWorker storageWorker = new StorageWorker();
        StorageService storage = new StorageWorkerService(storageWorker);
        createApp(storage).start(8080);
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