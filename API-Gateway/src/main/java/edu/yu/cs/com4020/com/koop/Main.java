package edu.yu.cs.com4020.com.koop;

import io.javalin.Javalin;
import java.io.InputStream;

public class Main {
    public static void main(String[] args) {
        // 1. Initialize the Service (Dependency Injection)
        String routerHost = System.getenv().getOrDefault("ROUTER_HOST", "localhost");
        int routerPort = Integer.parseInt(System.getenv().getOrDefault("ROUTER_PORT", "9000"));
        StorageService storage = new TcpStorageService(routerHost, routerPort);

        var app = Javalin.create(config -> {
            config.useVirtualThreads = true;
        }).start(8080);

        app.get("/health", ctx -> ctx.result("API Gateway is healthy!"));

        // GET
        app.get("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");

            InputStream data = storage.getObject(bucket, key);
            if (data != null) {
                ctx.result(data);
            } else {
                ctx.status(404).result("Object not found");
            }
        });

        // PUT
        app.put("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");

            try (InputStream is = ctx.bodyInputStream()) {
                storage.putObject(bucket, key, is);
                ctx.status(200).result("Object stored successfully");
            } catch (Exception e) {
                e.printStackTrace();
                ctx.status(500).result("Internal Error: " + e.getMessage());
            }
        });

        // DELETE
        app.delete("/{bucket}/{key}", ctx -> {
            try {
                storage.deleteObject(ctx.pathParam("bucket"), ctx.pathParam("key"));
                ctx.status(204);
            } catch (Exception e) {
                ctx.status(500).result("Error deleting object");
            }
        });
    }
}