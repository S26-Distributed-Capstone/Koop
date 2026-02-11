package com.github.koop.queryprocessor.gateway;

import io.javalin.Javalin;
import java.io.InputStream;

import com.github.koop.queryprocessor.gateway.StorageServices.LocalFileStorage;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.gateway.StorageServices.TcpStorageService;

public class Main {
    public static void main(String[] args) {
        // 1. Initialize the Service (Dependency Injection)
        String routerHost = System.getenv().getOrDefault("ROUTER_HOST", "localhost");
        int routerPort = Integer.parseInt(System.getenv().getOrDefault("ROUTER_PORT", "9000"));
        StorageService storage = new StorageWorkerService();


        var app = Javalin.create(config -> {
            config.useVirtualThreads = true;
        }).start(8080);

        app.get("/health", ctx -> ctx.result("API Gateway is healthy!"));

        // GET
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
                    // S3 Compatibility: XML Error for Missing File
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

        // PUT
        app.put("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            String resourcePath = "/" + bucket + "/" + key;


            try {
                // S3 Compatibility Fix 1: Do NOT use try-with-resources. 
                // Let Javalin close the stream when the request lifecycle ends.
                InputStream is = ctx.bodyInputStream();
                storage.putObject(bucket, key, is);
                
                // S3 Compatibility Fix 2: Empty body, 200 OK, and ETag header
                ctx.status(200);
                ctx.header("ETag", "\"dummy-etag-12345\""); // ETags must be wrapped in double quotes
                ctx.result(""); // strictly empty string
            } catch (Exception e) {
                e.printStackTrace();
                ctx.status(500);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
            }
        });

        // DELETE
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
    }

    // Helper method to generate S3-compliant XML error responses
    private static String buildS3ErrorXml(String code, String message, String resource) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<Error>\n" +
               "  <Code>" + code + "</Code>\n" +
               "  <Message>" + message + "</Message>\n" +
               "  <Resource>" + resource + "</Resource>\n" +
               "  <RequestId>" + java.util.UUID.randomUUID().toString() + "</RequestId>\n" +
               "</Error>";
    }
}