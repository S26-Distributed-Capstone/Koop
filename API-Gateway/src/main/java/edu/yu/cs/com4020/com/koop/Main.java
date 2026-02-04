package edu.yu.cs.com4020.com.koop;

import io.javalin.Javalin;

public class Main {
    public static void main(String[] args) {
        var app = Javalin.create().start(8080);

        app.get("/health", ctx -> {
            ctx.result("API Gateway is healthy!");
        });

        //S3 SKeleton Routes (place holder for now)

        //GET Object
        app.get("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            // Placeholder logic for getting an object from S3
            ctx.result("Getting object with key: " + key + " from bucket: " + bucket);
        });

        //PUT Object
        app.put("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            // Placeholder logic for putting an object to S3
            ctx.result("Putting object with key: " + key + " to bucket: " + bucket);
        });

        //DELETE Object
        app.delete("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            // Placeholder logic for deleting an object from S3
            ctx.result("Deleting object with key: " + key + " from bucket: " + bucket);
        });
        
    }
}