package com.github.koop.queryprocessor.gateway;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService.ObjectSummary;
import io.javalin.Javalin;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * S3 SDK Compatibility Tests V2 for the API Gateway (No-ETag Version).
 *
 * These tests use the official AWS SDK v2 S3 client pointed at a locally running
 * Javalin server to verify that the gateway behaves like a real S3-compatible
 * endpoint from the SDK's perspective, completely omitting ETag usage.
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3Test {

    @Mock
    private StorageService mockStorage;

    private Javalin app;
    private S3Client s3;
    private int port;

    // Fixed test constants
    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-object.txt";
    private static final String CONTENT = "Hello from S3 SDK without ETags!";
    private static final byte[] CONTENT_BYTES = CONTENT.getBytes(StandardCharsets.UTF_8);

    static java.util.logging.Level originalRootLevel;
    static java.util.logging.Level originalMainLevel;
    static java.util.logging.Handler[] rootHandlers;
    static java.util.logging.Level[] originalHandlerLevels;

    @BeforeAll
    static void setUpLogging() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(com.github.koop.queryprocessor.gateway.Main.class.getName());
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        originalMainLevel = logger.getLevel();
        originalRootLevel = rootLogger.getLevel();
        rootHandlers = rootLogger.getHandlers();
        originalHandlerLevels = new java.util.logging.Level[rootHandlers.length];
        for (int i = 0; i < rootHandlers.length; i++) {
            originalHandlerLevels[i] = rootHandlers[i].getLevel();
        }

        logger.setLevel(java.util.logging.Level.OFF);
        rootLogger.setLevel(java.util.logging.Level.WARNING);
        for (java.util.logging.Handler handler : rootHandlers) {
            handler.setLevel(java.util.logging.Level.WARNING);
        }
    }

    @AfterAll
    static void restoreLogging() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(com.github.koop.queryprocessor.gateway.Main.class.getName());
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        logger.setLevel(originalMainLevel);
        rootLogger.setLevel(originalRootLevel);
        for (int i = 0; i < rootHandlers.length; i++) {
            rootHandlers[i].setLevel(originalHandlerLevels[i]);
        }
    }

    @BeforeEach
    void setUp() {
        // Start the Javalin app on a random port
        app = Main.createApp(mockStorage).start(0);
        port = app.port();

        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("fake-access-key", "fake-secret-key")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .chunkedEncodingEnabled(false)
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
        assertTrue(port > 0, "App should have started on a valid port");
    }

    // ===================== PUT OBJECT =====================

    @Test
    @Order(2)
    void sdkPutObject_returns200_noException() throws Exception {
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        assertDoesNotThrow(() ->
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

        verify(mockStorage).putObject(eq(BUCKET), eq(KEY), any(), anyLong());
    }

    @Test
    @Order(4)
    void sdkPutObject_fromFile_passesContentLengthToStorageService(@TempDir Path tempDir) throws Exception {
        Path tempFile = tempDir.resolve("upload.txt");
        Files.write(tempFile, CONTENT_BYTES);
        long expectedLength = Files.size(tempFile);

        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        // Ensures the SDK maps the file size into Content-Length correctly, overriding chunking behavior if any
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key("file-upload.txt")
                        .contentLength(expectedLength) 
                        .build(),
                RequestBody.fromFile(tempFile)
        );

        // Verify that the exact file size was passed to the storage worker backend
        verify(mockStorage).putObject(eq(BUCKET), eq("file-upload.txt"), any(), eq(expectedLength));
    }

    @Test
    @Order(5)
    void sdkPutObject_storageThrows_raisesS3Exception() throws Exception {
        doThrow(new RuntimeException("disk full"))
                .when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

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
    @Order(6)
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
    @Order(7)
    void sdkGetObject_notFound_throwsNoSuchKeyException() throws Exception {
        when(mockStorage.getObject(BUCKET, "missing-key")).thenReturn(null);

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

        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());
        when(mockStorage.getObject(BUCKET, "lifecycle.bin"))
                .thenReturn(new ByteArrayInputStream(data))
                .thenReturn(null);
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        // PUT
        assertDoesNotThrow(() -> 
            s3.putObject(
                    PutObjectRequest.builder().bucket(BUCKET).key("lifecycle.bin")
                            .contentLength((long) data.length).build(),
                    RequestBody.fromBytes(data)
            )
        );

        // GET 
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

        // GET after delete
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
        byte[] largeData = new byte[1024 * 1024]; // 1MB
        new java.util.Random().nextBytes(largeData);

        doAnswer(invocation -> {
            java.io.InputStream is = invocation.getArgument(2);
            is.readAllBytes();
            return null;
        }).when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

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

    @Test
    void sdkPutObject_fromInputStream_passesContentLengthToStorageService() throws Exception {
        byte[] streamData = "data from an input stream".getBytes(StandardCharsets.UTF_8);
        long expectedLength = streamData.length;

        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(), anyLong());

        // Use RequestBody.fromInputStream and explicitly pass the length
        assertDoesNotThrow(() ->
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET)
                            .key("stream-upload.txt")
                            .build(),
                    RequestBody.fromInputStream(new ByteArrayInputStream(streamData), expectedLength)
            )
        );

        // Verify that the exact stream size was passed to the storage worker backend
        verify(mockStorage).putObject(eq(BUCKET), eq("stream-upload.txt"), any(), eq(expectedLength));
    }

    // ===================== BUCKET OPERATIONS =====================

    @Test
    @Order(16)
    void sdkCreateBucket_returns200_noException() throws Exception {
        doNothing().when(mockStorage).createBucket(anyString());

        assertDoesNotThrow(() ->
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(BUCKET)
                        .build())
        );
    }

    @Test
    @Order(17)
    void sdkCreateBucket_delegatesCorrectBucketName_toStorageService() throws Exception {
        doNothing().when(mockStorage).createBucket(anyString());

        s3.createBucket(CreateBucketRequest.builder().bucket("my-new-bucket").build());

        verify(mockStorage).createBucket("my-new-bucket");
    }

    @Test
    @Order(18)
    void sdkCreateBucket_storageThrows_raisesS3Exception() throws Exception {
        doThrow(new RuntimeException("bucket already exists"))
                .when(mockStorage).createBucket(anyString());

        assertThrows(S3Exception.class, () ->
                s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build())
        );
    }

    @Test
    @Order(19)
    void sdkDeleteBucket_returns204_noException() throws Exception {
        doNothing().when(mockStorage).deleteBucket(anyString());

        assertDoesNotThrow(() ->
                s3.deleteBucket(DeleteBucketRequest.builder()
                        .bucket(BUCKET)
                        .build())
        );
    }

    @Test
    @Order(20)
    void sdkDeleteBucket_delegatesCorrectBucketName_toStorageService() throws Exception {
        doNothing().when(mockStorage).deleteBucket(anyString());

        s3.deleteBucket(DeleteBucketRequest.builder().bucket("old-bucket").build());

        verify(mockStorage).deleteBucket("old-bucket");
    }

    @Test
    @Order(21)
    void sdkHeadBucket_bucketExists_returns200() throws Exception {
        when(mockStorage.bucketExists(BUCKET)).thenReturn(true);

        assertDoesNotThrow(() ->
                s3.headBucket(HeadBucketRequest.builder().bucket(BUCKET).build())
        );
    }

    @Test
    @Order(22)
    void sdkHeadBucket_bucketMissing_throwsNoSuchBucketException() throws Exception {
        when(mockStorage.bucketExists("ghost-bucket")).thenReturn(false);

        NoSuchBucketException ex = assertThrows(NoSuchBucketException.class, () ->
                s3.headBucket(HeadBucketRequest.builder().bucket("ghost-bucket").build())
        );

        assertEquals(404, ex.statusCode());
    }

    @Test
    @Order(23)
    void sdkListObjects_emptyBucket_returnsZeroContents() throws Exception {
        when(mockStorage.listObjects(eq(BUCKET), anyString(), anyInt()))
                .thenReturn(List.of());

        ListObjectsV2Response response = s3.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET).build()
        );

        assertEquals(0, response.keyCount(), "Empty bucket should return keyCount=0");
        assertTrue(response.contents().isEmpty(), "Empty bucket should return no Contents entries");
    }

    @Test
    @Order(24)
    void sdkListObjects_withObjects_returnsCorrectKeys() throws Exception {
        // ETag is omitted from the ObjectSummary 
        List<ObjectSummary> summaries = List.of(
                new ObjectSummary("file-a.txt", 100L, "2025-01-01T00:00:00Z"),
                new ObjectSummary("file-b.txt", 200L, "2025-01-02T00:00:00Z")
        );
        when(mockStorage.listObjects(eq(BUCKET), anyString(), anyInt()))
                .thenReturn(summaries);

        ListObjectsV2Response response = s3.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET).build()
        );

        assertEquals(2, response.keyCount());
        assertEquals("file-a.txt", response.contents().get(0).key());
        assertEquals("file-b.txt", response.contents().get(1).key());
    }

    @Test
    @Order(25)
    void sdkListObjects_passesPrefix_toStorageService() throws Exception {
        when(mockStorage.listObjects(eq(BUCKET), eq("logs/"), anyInt()))
                .thenReturn(List.of());

        s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .prefix("logs/")
                .build());

        verify(mockStorage).listObjects(eq(BUCKET), eq("logs/"), anyInt());
    }

    // ===================== MULTIPART UPLOAD =====================

    @Test
    @Order(26)
    void sdkCreateMultipartUpload_returnsUploadId() throws Exception {
        when(mockStorage.initiateMultipartUpload(BUCKET, KEY)).thenReturn("upload-id-xyz");

        CreateMultipartUploadResponse response = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .build()
        );

        assertNotNull(response.uploadId(), "SDK should receive and expose the uploadId");
        assertEquals("upload-id-xyz", response.uploadId());
    }

    @Test
    @Order(27)
    void sdkCreateMultipartUpload_delegatesCorrectBucketAndKey_toStorageService() throws Exception {
        when(mockStorage.initiateMultipartUpload(anyString(), anyString())).thenReturn("upload-id-xyz");

        s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket("videos")
                .key("movie.mp4")
                .build());

        verify(mockStorage).initiateMultipartUpload("videos", "movie.mp4");
    }

    @Test
    @Order(28)
    void sdkUploadPart_completesWithoutException() throws Exception {
        byte[] partData = new byte[5 * 1024 * 1024]; 
        doAnswer(invocation -> {
            java.io.InputStream is = invocation.getArgument(4);
            is.readAllBytes();
            return ""; // No ETag returned by StorageService
        }).when(mockStorage).uploadPart(eq(BUCKET), eq(KEY), eq("upload-id-xyz"),
                eq(1), any(), anyLong());

        assertDoesNotThrow(() -> 
            s3.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(BUCKET)
                            .key(KEY)
                            .uploadId("upload-id-xyz")
                            .partNumber(1)
                            .contentLength((long) partData.length)
                            .build(),
                    RequestBody.fromBytes(partData)
            )
        );
    }

    @Test
    @Order(29)
    void sdkUploadPart_delegatesCorrectArgs_toStorageService() throws Exception {
        byte[] partData = new byte[5 * 1024 * 1024];
        doAnswer(invocation -> {
            java.io.InputStream is = invocation.getArgument(4);
            is.readAllBytes();
            return ""; 
        }).when(mockStorage).uploadPart(anyString(), anyString(), anyString(), anyInt(), any(), anyLong());

        s3.uploadPart(
                UploadPartRequest.builder()
                        .bucket(BUCKET).key(KEY)
                        .uploadId("upload-id-xyz")
                        .partNumber(2)
                        .contentLength((long) partData.length)
                        .build(),
                RequestBody.fromBytes(partData)
        );

        verify(mockStorage).uploadPart(eq(BUCKET), eq(KEY), eq("upload-id-xyz"),
                eq(2), any(), anyLong());
    }

    @Test
    @Order(30)
    void sdkCompleteMultipartUpload_completesWithoutException() throws Exception {
        when(mockStorage.completeMultipartUpload(eq(BUCKET), eq(KEY), eq("upload-id-xyz"), anyList()))
                .thenReturn("");

        CompletedPart sdkPart = CompletedPart.builder().partNumber(1).build();

        assertDoesNotThrow(() -> 
            s3.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(BUCKET)
                            .key(KEY)
                            .uploadId("upload-id-xyz")
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(sdkPart)
                                    .build())
                            .build()
            )
        );
    }

    @Test
    @Order(31)
    void sdkCompleteMultipartUpload_delegatesCorrectUploadIdAndParts_toStorageService() throws Exception {
        when(mockStorage.completeMultipartUpload(anyString(), anyString(), anyString(), anyList()))
                .thenReturn("");

        CompletedPart part1 = CompletedPart.builder().partNumber(1).build();
        CompletedPart part2 = CompletedPart.builder().partNumber(2).build();

        s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(BUCKET).key(KEY)
                .uploadId("upload-id-xyz")
                .multipartUpload(CompletedMultipartUpload.builder().parts(part1, part2).build())
                .build());

        verify(mockStorage).completeMultipartUpload(
                eq(BUCKET), eq(KEY), eq("upload-id-xyz"),
                argThat(parts ->
                        parts.size() == 2
                        && parts.get(0).partNumber() == 1
                        && parts.get(1).partNumber() == 2
                )
        );
    }

    @Test
    @Order(32)
    void sdkAbortMultipartUpload_returns204_noException() throws Exception {
        doNothing().when(mockStorage).abortMultipartUpload(anyString(), anyString(), anyString());

        assertDoesNotThrow(() ->
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(KEY)
                        .uploadId("upload-id-xyz")
                        .build())
        );
    }

    @Test
    @Order(33)
    void sdkAbortMultipartUpload_delegatesCorrectArgs_toStorageService() throws Exception {
        doNothing().when(mockStorage).abortMultipartUpload(anyString(), anyString(), anyString());

        s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(BUCKET).key(KEY)
                .uploadId("upload-id-xyz")
                .build());

        verify(mockStorage).abortMultipartUpload(BUCKET, KEY, "upload-id-xyz");
    }

    // ===================== MULTIPART FULL LIFECYCLE =====================

    @Test
    @Order(34)
    void sdkMultipartLifecycle_initiateUploadCompleteThenGet() throws Exception {
        String uploadId = "lifecycle-upload-id";
        byte[] part1 = new byte[5 * 1024 * 1024];
        byte[] part2 = "final-chunk".getBytes(StandardCharsets.UTF_8);

        when(mockStorage.initiateMultipartUpload(BUCKET, KEY)).thenReturn(uploadId);
        doAnswer(invocation -> {
            java.io.InputStream is = invocation.getArgument(4);
            is.readAllBytes();
            return ""; 
        }).when(mockStorage).uploadPart(eq(BUCKET), eq(KEY), eq(uploadId), eq(1), any(), anyLong());
        doAnswer(invocation -> {
            java.io.InputStream is = invocation.getArgument(4);
            is.readAllBytes();
            return "";
        }).when(mockStorage).uploadPart(eq(BUCKET), eq(KEY), eq(uploadId), eq(2), any(), anyLong());
        when(mockStorage.completeMultipartUpload(eq(BUCKET), eq(KEY), eq(uploadId), anyList()))
                .thenReturn("");
        when(mockStorage.getObject(BUCKET, KEY))
                .thenReturn(new ByteArrayInputStream("assembled".getBytes(StandardCharsets.UTF_8)));

        // Step 1: initiate
        CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(BUCKET).key(KEY).build()
        );
        assertEquals(uploadId, initResp.uploadId());

        // Step 2: upload parts
        s3.uploadPart(
                UploadPartRequest.builder().bucket(BUCKET).key(KEY)
                        .uploadId(uploadId).partNumber(1).contentLength((long) part1.length).build(),
                RequestBody.fromBytes(part1)
        );
        s3.uploadPart(
                UploadPartRequest.builder().bucket(BUCKET).key(KEY)
                        .uploadId(uploadId).partNumber(2).contentLength((long) part2.length).build(),
                RequestBody.fromBytes(part2)
        );

        // Step 3: complete (ETags omitted from parts)
        assertDoesNotThrow(() -> 
            s3.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(BUCKET).key(KEY).uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder().parts(
                                    CompletedPart.builder().partNumber(1).build(),
                                    CompletedPart.builder().partNumber(2).build()
                            ).build())
                            .build()
            )
        );

        // Step 4: verify assembled object is retrievable
        ResponseBytes<GetObjectResponse> getResp = s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(KEY).build(),
                ResponseTransformer.toBytes()
        );
        assertNotNull(getResp.asByteArray());
    }
}