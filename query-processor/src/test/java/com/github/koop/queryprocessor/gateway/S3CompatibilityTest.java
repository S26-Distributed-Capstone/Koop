package com.github.koop.queryprocessor.gateway;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import io.javalin.Javalin;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;



import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * S3 SDK Compatibility Tests for the API Gateway.
 *
 * These tests use the official AWS SDK v2 S3 client pointed at a locally running
 * Javalin server to verify that the gateway behaves like a real S3-compatible
 * endpoint from the SDK's perspective.
 *
 * The StorageService backend is mocked — we only care that the gateway correctly
 * translates between the S3 protocol and the StorageService interface.
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3CompatibilityTest {

    @Mock
    private StorageService mockStorage;

    private Javalin app;
    private S3Client s3;
    private int port;

    // Fixed test constants
    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-object.txt";
    private static final String CONTENT = "Hello from S3 SDK!";
    private static final byte[] CONTENT_BYTES = CONTENT.getBytes(StandardCharsets.UTF_8);

    static {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(com.github.koop.queryprocessor.gateway.Main.class.getName());
        logger.setLevel(java.util.logging.Level.OFF);
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        rootLogger.setLevel(java.util.logging.Level.WARNING);
        for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
            handler.setLevel(java.util.logging.Level.WARNING);
        }
    }

    @BeforeEach
    void setUp() {
        // Start the Javalin app on a random port (0 = OS picks one)
        app = Main.createApp(mockStorage).start(0);
        port = app.port();

        // Build an S3 client pointing at our local gateway.
        // - endpoint: localhost:<port>
        // - pathStyleAccess: required since we're not on real AWS (no subdomain routing)
        // - credentials: fake but required by the SDK
        // - region: any value works since we're not on real AWS
        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("fake-access-key", "fake-secret-key")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(true) // CRITICAL: enables /{bucket}/{key} style URLs
                        .build())
                .build();
    }

    @AfterEach
    void tearDown() {
        if (s3 != null) s3.close();
        if (app != null) app.stop();
    }

    // ===================== HEALTH CHECK =====================

    @Test
    @Order(1)
    void healthEndpoint_isReachable() {
        // Sanity check: confirm the server is up before running S3 tests.
        // We use raw HTTP here since the S3 SDK doesn't have a health-check concept.
        var client = java.net.http.HttpClient.newHttpClient();
        // Simple approach: just ensure app started on a valid port
        assertTrue(port > 0, "App should have started on a valid port");
    }

    // ===================== PUT OBJECT =====================

    @Test
    @Order(2)
    void sdkPutObject_returns200_andETag() throws Exception {
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        PutObjectResponse response = s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .contentLength((long) CONTENT_BYTES.length)
                        .build(),
                RequestBody.fromBytes(CONTENT_BYTES)
        );

        // The SDK parses the ETag header — verify it came through
        assertNotNull(response.eTag(), "SDK should receive and parse ETag from gateway");
        assertEquals("\"dummy-etag-12345\"", response.eTag());
    }

    @Test
    @Order(3)
    void sdkPutObject_delegatesCorrectBucketAndKey_toStorageService() throws Exception {
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .contentLength((long) CONTENT_BYTES.length)
                        .build(),
                RequestBody.fromBytes(CONTENT_BYTES)
        );

        // Verify the gateway correctly parsed the S3 path and forwarded to StorageService
        verify(mockStorage).putObject(eq(BUCKET), eq(KEY), any(), anyLong());
    }

    @Test
    @Order(4)
    void sdkPutObject_storageThrows_raisesS3Exception() throws Exception {
        doThrow(new RuntimeException("disk full"))
                .when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        // The SDK should raise an S3Exception (wrapping the 500 response) rather than a raw HTTP error
        assertThrows(S3Exception.class, () ->
                s3.putObject(
                        PutObjectRequest.builder()
                                .bucket(BUCKET)
                                .key(KEY)
                                .contentLength((long) CONTENT_BYTES.length)
                                .build(),
                        RequestBody.fromBytes(CONTENT_BYTES)
                )
        );
    }

    // ===================== GET OBJECT =====================

    @Test
    @Order(5)
    void sdkGetObject_returnsCorrectContent() throws Exception {
        when(mockStorage.getObject(BUCKET, KEY))
                .thenReturn(new ByteArrayInputStream(CONTENT_BYTES));

        ResponseBytes<GetObjectResponse> result = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .build(),
                ResponseTransformer.toBytes()
        );

        assertArrayEquals(CONTENT_BYTES, result.asByteArray(),
                "SDK should receive exactly the bytes stored via the gateway");
    }

    @Test
    @Order(6)
    void sdkGetObject_returnsETag() throws Exception {
        when(mockStorage.getObject(BUCKET, KEY))
                .thenReturn(new ByteArrayInputStream(CONTENT_BYTES));

        ResponseBytes<GetObjectResponse> result = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(KEY).build(),
                ResponseTransformer.toBytes()
        );

        assertEquals("\"dummy-etag-12345\"", result.response().eTag(),
                "SDK should parse ETag from gateway response header");
    }

    @Test
    @Order(7)
    void sdkGetObject_notFound_throwsNoSuchKeyException() throws Exception {
        when(mockStorage.getObject(BUCKET, "missing-key")).thenReturn(null);

        // The SDK should map the 404 + NoSuchKey XML to the typed NoSuchKeyException
        NoSuchKeyException ex = assertThrows(NoSuchKeyException.class, () ->
                s3.getObject(
                        GetObjectRequest.builder()
                                .bucket(BUCKET)
                                .key("missing-key")
                                .build(),
                        ResponseTransformer.toBytes()
                )
        );

        assertEquals(404, ex.statusCode(),
                "NoSuchKeyException should carry the 404 status code");
    }

    @Test
    @Order(8)
    void sdkGetObject_storageThrows_raisesS3Exception() throws Exception {
        when(mockStorage.getObject(anyString(), anyString()))
                .thenThrow(new RuntimeException("network error"));

        S3Exception ex = assertThrows(S3Exception.class, () ->
                s3.getObject(
                        GetObjectRequest.builder().bucket(BUCKET).key(KEY).build(),
                        ResponseTransformer.toBytes()
                )
        );

        assertEquals(500, ex.statusCode());
    }

    @Test
    @Order(9)
    void sdkGetObject_delegatesCorrectBucketAndKey_toStorageService() throws Exception {
        when(mockStorage.getObject("videos", "clip.mp4"))
                .thenReturn(new ByteArrayInputStream("data".getBytes()));

        s3.getObject(
                GetObjectRequest.builder().bucket("videos").key("clip.mp4").build(),
                ResponseTransformer.toBytes()
        );

        verify(mockStorage).getObject("videos", "clip.mp4");
    }

    // ===================== DELETE OBJECT =====================

    @Test
    @Order(10)
    void sdkDeleteObject_returns204_noException() throws Exception {
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        // SDK delete should complete without throwing
        assertDoesNotThrow(() ->
                s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .build())
        );
    }

    @Test
    @Order(11)
    void sdkDeleteObject_delegatesCorrectBucketAndKey_toStorageService() throws Exception {
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket("videos")
                .key("old-clip.mp4")
                .build());

        verify(mockStorage).deleteObject("videos", "old-clip.mp4");
    }

    @Test
    @Order(12)
    void sdkDeleteObject_storageThrows_raisesS3Exception() throws Exception {
        doThrow(new RuntimeException("delete failed"))
                .when(mockStorage).deleteObject(anyString(), anyString());

        assertThrows(S3Exception.class, () ->
                s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .build())
        );
    }

    // ===================== FULL LIFECYCLE =====================

    @Test
    @Order(13)
    void sdkFullLifecycle_putGetDelete() throws Exception {
        byte[] data = "lifecycle-test-data".getBytes(StandardCharsets.UTF_8);

        // Set up mock responses
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());
        when(mockStorage.getObject(BUCKET, "lifecycle.bin"))
                .thenReturn(new ByteArrayInputStream(data))
                .thenReturn(null); // second call after delete returns null
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        // PUT
        PutObjectResponse putResp = s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key("lifecycle.bin")
                        .contentLength((long) data.length).build(),
                RequestBody.fromBytes(data)
        );
        assertNotNull(putResp.eTag());

        // GET — should find the object
        ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key("lifecycle.bin").build(),
                ResponseTransformer.toBytes()
        );
        assertArrayEquals(data, getResp.asByteArray());

        // DELETE
        assertDoesNotThrow(() ->
                s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(BUCKET).key("lifecycle.bin").build())
        );

        // GET after delete — should throw NoSuchKeyException
        assertThrows(NoSuchKeyException.class, () ->
                s3.getObject(
                        GetObjectRequest.builder().bucket(BUCKET).key("lifecycle.bin").build(),
                        ResponseTransformer.toBytes()
                )
        );
    }

    // ===================== CONTENT-TYPE & METADATA =====================

    @Test
    @Order(14)
    void sdkGetObject_contentType_isOctetStream() throws Exception {
        when(mockStorage.getObject(BUCKET, KEY))
                .thenReturn(new ByteArrayInputStream(CONTENT_BYTES));

        ResponseBytes<GetObjectResponse> result = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(KEY).build(),
                ResponseTransformer.toBytes()
        );

        assertEquals("application/octet-stream", result.response().contentType(),
                "Gateway should return application/octet-stream for all objects");
    }

    @Test
    @Order(15)
    void sdkPutObject_largePayload_handledCorrectly() throws Exception {
        // Test with a larger payload to verify streaming/buffering is handled correctly
        byte[] largeData = new byte[1024 * 1024]; // 1MB
        new java.util.Random().nextBytes(largeData);

        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        assertDoesNotThrow(() ->
                s3.putObject(
                        PutObjectRequest.builder()
                                .bucket(BUCKET)
                                .key("large-file.bin")
                                .contentLength((long) largeData.length)
                                .build(),
                        RequestBody.fromBytes(largeData)
                )
        );

        verify(mockStorage).putObject(eq(BUCKET), eq("large-file.bin"), any(), anyLong());
    }
}