package com.github.koop.queryprocessor.gateway;

import io.javalin.Javalin;
import java.io.InputStream;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.StorageWorker;

public class Main {

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
                e.printStackTrace();
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
                e.printStackTrace();
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
                e.printStackTrace();
                ctx.status(500);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
            }
        });

        return app;
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