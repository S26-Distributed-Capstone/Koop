package edu.yu.cs.com4020.com.koop;

import io.javalin.Javalin;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Main {

    private static final String STORAGE_DIR ="/app/storage"; // Directory to store S3 objects


    public static void main(String[] args) {
        try{
            Files.createDirectories(Paths.get(STORAGE_DIR));
        }catch(Exception e){
            System.err.println("Failed to create storage directory: " + e.getMessage());
        }

        var app = Javalin.create().start(8080);

        app.get("/health", ctx -> {
            ctx.result("API Gateway is healthy!");
        });

        //S3 SKeleton Routes (place holder for now)

        //GET Object (download a file)
        app.get("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            
            Path filePath = Paths.get(STORAGE_DIR, bucket, key);
            if(Files.exists(filePath)){
                ctx.result(Files.newInputStream(filePath));
            }else{
                ctx.status(404).result("Object not found");
            }
        });

        //PUT Object (upload a file)
        app.put("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            
            //create bucket directory if it doesn't exist
            Path bucketPath = Paths.get(STORAGE_DIR, bucket);
            Files.createDirectories(bucketPath);

            //read the input stream and save the file
            try(InputStream is = ctx.bodyInputStream()){
                Path dest = bucketPath.resolve(key);
                Files.copy(is, dest, StandardCopyOption.REPLACE_EXISTING);

                System.out.println("Stored file at: " + dest.toString());
                ctx.status(200).result("Object stored successfully");
            }catch(Exception e){
                e.printStackTrace();
                ctx.status(500).result("Internal Error:" + e.getMessage());
            }

        });

        //DELETE Object
        app.delete("/{bucket}/{key}", ctx -> {
            String bucket = ctx.pathParam("bucket");
            String key = ctx.pathParam("key");
            
            Path filePath = Paths.get(STORAGE_DIR, bucket, key);
            if(Files.exists(filePath)){
                Files.delete(filePath);
                ctx.status(204);
            }else{
                ctx.status(404).result("Object not found");
            }
        });
        
    }
}