package koop;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DockerComposeS3E2EIT {

    private static final String BUCKET = "e2e-bucket";
    private S3Client s3;

    @Container
    public static ComposeContainer environment =
            new ComposeContainer(new File("../docker-compose.yml"))
                    .withBuild(true);

    private static void log(String msg) {
        System.out.printf("[%s] [E2E-LOG] %s%n", LocalTime.now().withNano(0), msg);
    }

    @BeforeAll
    void setupS3Client() {
        log("======================================================");
        log("PHASE: SETUP & BOOTSTRAP");
        log("======================================================");
        log("1. Testcontainers is spinning up docker-compose.yml...");
        log("2. Bypassing SOCAT proxy. Connecting directly to localhost:9001");

        String host = "127.0.0.1";
        Integer port = 9001;

        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://" + host + ":" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("fake-access-key", "fake-secret-key")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .chunkedEncodingEnabled(false)
                        .build())
                .build();

        log("3. Polling the Gateway S3 API to check if it is awake (Max 3 minutes)...");
        long startTime = System.currentTimeMillis();
        boolean isUp = false;

        while (System.currentTimeMillis() - startTime < 180_000) {
            try {
                s3.headBucket(HeadBucketRequest.builder().bucket("health-check-probe").build());
                isUp = true;
                break;
            } catch (NoSuchBucketException e) {
                isUp = true;
                break;
            } catch (Exception e) {
                try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
            }
        }

        if (!isUp) {
            throw new RuntimeException("Gateway API at port 9001 never responded.");
        }

        log("4. SUCCESS: Gateway API is online and accepting connections!");

        log("5. Checking if base testing bucket '" + BUCKET + "' exists...");
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(BUCKET).build());
            log("-> Bucket already exists. Moving on.");
        } catch (NoSuchBucketException e) {
            log("-> Bucket missing. Creating bucket '" + BUCKET + "' now...");
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            log("-> Bucket created successfully.");
        }
        log("SETUP COMPLETE. Starting test suite.\n");
    }

    @AfterAll
    void teardown() {
        log("======================================================");
        log("PHASE: TEARDOWN");
        log("======================================================");
        if (s3 != null) {
            log("Closing S3 Java Client connection...");
            s3.close();
        }
        if (environment != null) {
            log("Commanding Testcontainers to destroy the Docker Compose environment...");
            environment.stop();
            log("Environment destroyed successfully.");
        }
    }

    @Test
    void e2e_putObject_thenGetObject_roundTrip() {
        log("\n--- TEST: Simple PUT -> GET Round Trip ---");
        byte[] data = new byte[2 * 1024 * 1024]; // 2 MB
        new SecureRandom().nextBytes(data);
        String key = "roundtrip.bin";

        log("Step 1: Dispatching PUT request to Gateway (2MB payload) for key: " + key);
        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(key).contentLength((long) data.length).build(),
                RequestBody.fromBytes(data)
        );
        log("Step 1: PUT request acknowledged by Gateway.");

        log("Step 2: Dispatching GET request to retrieve " + key);
        ResponseBytes<GetObjectResponse> response = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );

        byte[] got = response.asByteArray();
        log("Step 2: GET request returned " + got.length + " bytes.");

        assertArrayEquals(data, got, "GET should return exactly the bytes that were PUT");
        log("--- TEST PASSED ---\n");
    }

    @Test
    void e2e_putObject_thenDeleteObject_thenGet_returns404() {
        log("\n--- TEST: PUT -> DELETE -> GET (404) ---");
        byte[] data = "delete-me-data".getBytes(StandardCharsets.UTF_8);
        String key = "delete-test.txt";

        log("Step 1: Putting object " + key);
        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(key).contentLength((long) data.length).build(),
                RequestBody.fromBytes(data)
        );

        log("Step 2: Sending DELETE command for " + key);
        s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
        log("Step 2: DELETE confirmed.");

        log("Step 3: Sending GET request for deleted object (expecting 404 Exception)...");
        NoSuchKeyException ex = assertThrows(NoSuchKeyException.class, () ->
                s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build(), ResponseTransformer.toBytes())
        );
        log("Step 3: Gateway correctly returned 404 Status Code: " + ex.statusCode());
        assertEquals(404, ex.statusCode());
        log("--- TEST PASSED ---\n");
    }

    @Test
    void e2e_multipartUpload_fullLifecycle_thenGet() {
        log("\n--- TEST: Full Multipart Upload Lifecycle ---");
        String key = "mpu-lifecycle.bin";
        int partSize = 512 * 1024;
        int numParts = 3;

        byte[][] partData = new byte[numParts][];
        for (int i = 0; i < numParts; i++) {
            partData[i] = new byte[partSize];
            new SecureRandom().nextBytes(partData[i]);
        }

        log("Step 1: Sending CreateMultipartUpload request...");
        CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(BUCKET).key(key).build()
        );
        String uploadId = initResp.uploadId();
        log("Step 1: Received Upload ID from Gateway: " + uploadId);

        log("Step 2: Uploading " + numParts + " individual parts...");
        for (int i = 0; i < numParts; i++) {
            log(" -> Sending Part " + (i + 1) + " (" + partData[i].length + " bytes)...");
            s3.uploadPart(
                    UploadPartRequest.builder().bucket(BUCKET).key(key).uploadId(uploadId).partNumber(i + 1).contentLength((long) partData[i].length).build(),
                    RequestBody.fromBytes(partData[i])
            );
        }

        log("Step 3: Sending CompleteMultipartUpload request to stitch parts together...");
        List<CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            completedParts.add(CompletedPart.builder().partNumber(i + 1).build());
        }

        s3.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder().bucket(BUCKET).key(key).uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                        .build()
        );
        log("Step 3: Multipart upload completed and finalized by Gateway.");

        log("Step 4: Sending GET request to verify stitched file integrity...");
        ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                ResponseTransformer.toBytes()
        );

        byte[] expected = new byte[numParts * partSize];
        for (int i = 0; i < numParts; i++) {
            System.arraycopy(partData[i], 0, expected, i * partSize, partSize);
        }

        log("Step 4: Received " + getResp.asByteArray().length + " bytes. Verifying byte-by-byte match...");
        assertArrayEquals(expected, getResp.asByteArray());
        log("--- TEST PASSED ---\n");
    }

    @Test
    void e2e_createThenDeleteBucket_headReturns404() {
        log("\n--- TEST: Bucket Lifecycle (Create -> Delete -> Head) ---");
        String bucket = "lifecycle-" + System.nanoTime();

        log("Step 1: Sending CreateBucket request for: " + bucket);
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        log("Step 2: Sending DeleteBucket request for: " + bucket);
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());

        log("Step 3: Sending HeadBucket request (expecting 404 Exception)...");
        NoSuchBucketException ex = assertThrows(NoSuchBucketException.class, () ->
                s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
        );
        log("Step 3: Gateway correctly returned 404 Status Code: " + ex.statusCode());
        assertEquals(404, ex.statusCode());
        log("--- TEST PASSED ---\n");
    }
}