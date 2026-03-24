package koop;
import com.github.koop.queryprocessor.gateway.Main;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.StorageWorker;
import com.github.koop.storagenode.StorageNodeServer;
import io.javalin.Javalin;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class S3LocalE2EDemo {

    public static void main(String[] args) throws Exception {
        int totalNodes = 9;
        List<StorageNodeServer> servers = new ArrayList<>();
        List<InetSocketAddress> addrs = new ArrayList<>();

        System.out.println("--- Bootstrapping Storage Cluster ---");
        // 1. Start Storage Nodes on random free ports
        for (int i = 0; i < totalNodes; i++) {
            int port;
            try (ServerSocket ss = new ServerSocket(0)) {
                ss.setReuseAddress(true);
                port = ss.getLocalPort();
            }
            
            Path dir = Files.createTempDirectory("storagenode-s3-demo-" + i + "-");
            StorageNodeServer server = new StorageNodeServer(port, dir);
            server.start(); 
            
            servers.add(server);
            addrs.add(new InetSocketAddress("127.0.0.1", port));
        }

        System.out.println("--- Starting API Gateway ---");
        // 2. Initialize the Query Processor / API Gateway
        StorageWorker worker = new StorageWorker(addrs, addrs, addrs);
        StorageService storageService = new StorageWorkerService(worker);
        
        Javalin gateway = Main.createApp(storageService).start(9001);
        System.out.println("Gateway listening on http://localhost:9001\n");

        System.out.println("--- Initializing AWS S3 Client ---");
        // 3. Configure the official S3 Client to point to the local Gateway
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:9001"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("mock-access-key", "mock-secret-key")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true) // Required for local/IP endpoints
                        .chunkedEncodingEnabled(false) // Required for current Gateway implementation
                        .build())
                .build()) {

            String bucketName = "s3-demo-bucket";
            String objectKey = "s3-demo-object.txt";
            String payloadData = "Data written via the official AWS S3 SDK v2!";

            System.out.println("\n>>> 1. Create Bucket");
            //TODO: Not yet implemented
            //s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            //System.out.println("Bucket '" + bucketName + "' created.");

            System.out.println("\n>>> 2. Put Object");
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .contentLength((long) payloadData.getBytes(StandardCharsets.UTF_8).length)
                            .build(),
                    RequestBody.fromString(payloadData)
            );
            System.out.println("Object '" + objectKey + "' uploaded successfully.");

            System.out.println("\n>>> 3. Get Object");
            ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .build(),
                    ResponseTransformer.toBytes()
            );
            System.out.println("Retrieved Content: " + getResp.asUtf8String());

            System.out.println("\n>>> 4. Delete Object");
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build());
            System.out.println("Object deleted.");

            System.out.println("\n>>> 5. Verify Deletion (Expect NoSuchKeyException)");
            try {
                s3.getObject(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(objectKey)
                                .build(),
                        ResponseTransformer.toBytes()
                );
            } catch (NoSuchKeyException e) {
                System.out.println("Success: Caught expected NoSuchKeyException (HTTP 404).");
            }
        }

        // 4. Teardown
        System.out.println("\n--- Shutting down cluster ---");
        gateway.stop();
        for (StorageNodeServer server : servers) {
            server.stop();
        }
        System.out.println("Shutdown complete.");
    }
}