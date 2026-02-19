package com.github.koop.queryprocessor.gateway;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GatewayHttpTest {

    @Mock
    StorageService mockStorage;

    // ===================== HEALTH CHECK =====================

    @Test
    void healthCheck_returns200() {
        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.get("/health");
            assertEquals(200, response.code());
            assertEquals("API Gateway is healthy!", response.body().string());
        });
    }

    // ===================== PUT OBJECT =====================

    @Test
    void putObject_returns200_withETag() throws Exception {
        // StorageService.putObject is void — just don't throw
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(InputStream.class), anyLong());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.request("/my-bucket/my-key", builder ->
                builder.put(okhttp3.RequestBody.create("hello world".getBytes(), okhttp3.MediaType.get("application/octet-stream")))
            );

            assertEquals(200, response.code());
            assertNotNull(response.header("ETag"));
            assertEquals("\"dummy-etag-12345\"", response.header("ETag"));
            assertEquals("", response.body().string());  // S3 compat: empty body
        });
    }

    @Test
    void putObject_delegatesToStorageService_withCorrectBucketAndKey() throws Exception {
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(InputStream.class), anyLong());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            client.request("/videos/movie.mp4", builder ->
                builder.put(okhttp3.RequestBody.create("data".getBytes(), okhttp3.MediaType.get("application/octet-stream")))
            );

            verify(mockStorage).putObject(eq("videos"), eq("movie.mp4"), any(InputStream.class), anyLong());
        });
    }

    @Test
    void putObject_storageThrows_returns500_withS3ErrorXml() throws Exception {
        doThrow(new RuntimeException("StorageWorker failed to store object"))
            .when(mockStorage).putObject(anyString(), anyString(), any(InputStream.class), anyLong());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.request("/bucket/key", builder ->
                builder.put(okhttp3.RequestBody.create("data".getBytes(), okhttp3.MediaType.get("application/octet-stream")))
            );

            assertEquals(500, response.code());
            assertEquals("application/xml", response.header("Content-Type"));
            String body = response.body().string();
            assertTrue(body.contains("<Code>InternalError</Code>"));
            assertTrue(body.contains("<Resource>/bucket/key</Resource>"));
        });
    }

    // ===================== GET OBJECT =====================

    @Test
    void getObject_returns200_withContent() throws Exception {
        byte[] content = "video-bytes-here".getBytes(StandardCharsets.UTF_8);
        when(mockStorage.getObject("my-bucket", "my-key"))
            .thenReturn(new ByteArrayInputStream(content));

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.get("/my-bucket/my-key");

            assertEquals(200, response.code());
            assertEquals("application/octet-stream", response.header("Content-Type"));
            assertEquals("\"dummy-etag-12345\"", response.header("ETag"));
            assertArrayEquals(content, response.body().bytes());
        });
    }

    @Test
    void getObject_notFound_returns404_withNoSuchKeyXml() throws Exception {
        when(mockStorage.getObject("bucket", "missing-key"))
            .thenReturn(null);

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.get("/bucket/missing-key");

            assertEquals(404, response.code());
            assertEquals("application/xml", response.header("Content-Type"));
            String body = response.body().string();
            assertTrue(body.contains("<Code>NoSuchKey</Code>"));
            assertTrue(body.contains("The specified key does not exist."));
            assertTrue(body.contains("<Resource>/bucket/missing-key</Resource>"));
        });
    }

    @Test
    void getObject_storageThrows_returns500_withS3ErrorXml() throws Exception {
        when(mockStorage.getObject(anyString(), anyString()))
            .thenThrow(new RuntimeException("connection failed"));

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.get("/bucket/key");

            assertEquals(500, response.code());
            assertEquals("application/xml", response.header("Content-Type"));
            String body = response.body().string();
            assertTrue(body.contains("<Code>InternalError</Code>"));
        });
    }

    @Test
    void getObject_delegatesToStorageService_withCorrectBucketAndKey() throws Exception {
        when(mockStorage.getObject("videos", "clip.mp4"))
            .thenReturn(new ByteArrayInputStream("data".getBytes()));

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            client.get("/videos/clip.mp4");

            verify(mockStorage).getObject("videos", "clip.mp4");
        });
    }

    // ===================== DELETE OBJECT =====================

    @Test
    void deleteObject_returns204() throws Exception {
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.request("/bucket/key", builder ->
                builder.delete()
            );

            assertEquals(204, response.code());
        });
    }

    @Test
    void deleteObject_delegatesToStorageService_withCorrectBucketAndKey() throws Exception {
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            client.request("/videos/old-clip.mp4", builder -> builder.delete());

            verify(mockStorage).deleteObject("videos", "old-clip.mp4");
        });
    }

    @Test
    void deleteObject_storageThrows_returns500_withS3ErrorXml() throws Exception {
        doThrow(new RuntimeException("StorageWorker failed to delete object"))
            .when(mockStorage).deleteObject(anyString(), anyString());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.request("/bucket/key", builder ->
                builder.delete()
            );

            assertEquals(500, response.code());
            assertEquals("application/xml", response.header("Content-Type"));
            String body = response.body().string();
            assertTrue(body.contains("<Code>InternalError</Code>"));
            assertTrue(body.contains("<Resource>/bucket/key</Resource>"));
        });
    }

    // ===================== S3 XML ERROR FORMAT =====================

    @Test
    void s3ErrorXml_containsRequestId() throws Exception {
        when(mockStorage.getObject(anyString(), anyString())).thenReturn(null);

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            var response = client.get("/bucket/key");
            String body = response.body().string();
            assertTrue(body.contains("<RequestId>"));
            assertTrue(body.contains("</RequestId>"));
        });
    }

    // ===================== FULL LIFECYCLE =====================

    @Test
    void fullLifecycle_putGetDelete() throws Exception {
        byte[] content = "my-video-data".getBytes(StandardCharsets.UTF_8);

        // PUT succeeds
        doNothing().when(mockStorage).putObject(anyString(), anyString(), any(InputStream.class), anyLong());

        // First GET returns data, second GET (after delete) returns null
        when(mockStorage.getObject("bucket", "file.mp4"))
            .thenReturn(new ByteArrayInputStream(content))
            .thenReturn(null);

        // DELETE succeeds
        doNothing().when(mockStorage).deleteObject(anyString(), anyString());

        JavalinTest.test(Main.createApp(mockStorage), (server, client) -> {
            // PUT
            var putResp = client.request("/bucket/file.mp4", builder ->
                builder.put(okhttp3.RequestBody.create(content, okhttp3.MediaType.get("application/octet-stream")))
            );
            assertEquals(200, putResp.code());

            // GET — should find it
            var getResp = client.get("/bucket/file.mp4");
            assertEquals(200, getResp.code());
            assertArrayEquals(content, getResp.body().bytes());

            // DELETE
            var delResp = client.request("/bucket/file.mp4", builder -> builder.delete());
            assertEquals(204, delResp.code());

            // GET again — should 404
            var getResp2 = client.get("/bucket/file.mp4");
            assertEquals(404, getResp2.code());
        });
    }
}