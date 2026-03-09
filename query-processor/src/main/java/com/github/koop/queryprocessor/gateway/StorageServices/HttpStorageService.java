package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * HTTP-based client for the Storage Node.
 *
 * Object operations are fully wired to the storage node's REST API:
 *   PUT    /store/{partition}/{key}?requestId=...
 *   GET    /store/{partition}/{key}
 *   DELETE /store/{partition}/{key}
 *
 * Bucket and multipart-upload operations are stubbed — they throw
 * {@link UnsupportedOperationException} until the storage node exposes
 * the corresponding endpoints.
 */
public class HttpStorageService implements StorageService {

    private final String baseUrl;
    private final HttpClient httpClient;

    public HttpStorageService(String routerHost, int routerPort) {
        this.baseUrl = "http://" + routerHost + ":" + routerPort;
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    // ─── Object Operations ────────────────────────────────────────────────────

    @Override
    public void putObject(String bucket, String key, InputStream data, long length) throws Exception {
        String requestId = UUID.randomUUID().toString();
        int partition = getPartition(key);

        byte[] body = data.readAllBytes();

        URI uri = URI.create(String.format("%s/store/%d/%s?requestId=%s",
                baseUrl, partition, key, requestId));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/octet-stream")
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Storage Node returned HTTP " + response.statusCode() + " for PUT.");
        }
    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        int partition = getPartition(key);

        URI uri = URI.create(String.format("%s/store/%d/%s", baseUrl, partition, key));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        if (response.statusCode() == 404) {
            return null;
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("Storage Node returned HTTP " + response.statusCode() + " for GET.");
        }

        return new ByteArrayInputStream(response.body());
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        int partition = getPartition(key);

        URI uri = URI.create(String.format("%s/store/%d/%s", baseUrl, partition, key));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .DELETE()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Storage Node returned HTTP " + response.statusCode() + " for DELETE.");
        }
    }

    // ─── Bucket Operations (stubbed until storage node exposes bucket API) ────

    @Override
    public void createBucket(String bucket) throws Exception {
        // TODO: wire to POST /buckets/{bucket} (or equivalent) on the storage node
        throw new UnsupportedOperationException("createBucket not yet implemented in storage node");
    }

    @Override
    public void deleteBucket(String bucket) throws Exception {
        // TODO: wire to DELETE /buckets/{bucket} on the storage node
        throw new UnsupportedOperationException("deleteBucket not yet implemented in storage node");
    }

    @Override
    public List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception {
        // TODO: wire to GET /buckets/{bucket}/objects?prefix=...&maxKeys=... on the storage node
        throw new UnsupportedOperationException("listObjects not yet implemented in storage node");
    }

    @Override
    public boolean bucketExists(String bucket) throws Exception {
        // TODO: wire to HEAD /buckets/{bucket} on the storage node
        throw new UnsupportedOperationException("bucketExists not yet implemented in storage node");
    }

    // ─── Multipart Upload Operations (stubbed until storage node exposes multipart API) ───

    @Override
    public String initiateMultipartUpload(String bucket, String key) throws Exception {
        // TODO: wire to POST /multipart/{bucket}/{key}/initiate on the storage node
        throw new UnsupportedOperationException("initiateMultipartUpload not yet implemented in storage node");
    }

    @Override
    public String uploadPart(String bucket, String key, String uploadId,
                             int partNumber, InputStream data, long length) throws Exception {
        // TODO: wire to PUT /multipart/{bucket}/{key}?uploadId=...&partNumber=... on the storage node
        throw new UnsupportedOperationException("uploadPart not yet implemented in storage node");
    }

    @Override
    public String completeMultipartUpload(String bucket, String key, String uploadId,
                                          List<CompletedPart> parts) throws Exception {
        // TODO: wire to POST /multipart/{bucket}/{key}/complete?uploadId=... on the storage node
        throw new UnsupportedOperationException("completeMultipartUpload not yet implemented in storage node");
    }

    @Override
    public void abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        // TODO: wire to DELETE /multipart/{bucket}/{key}?uploadId=... on the storage node
        throw new UnsupportedOperationException("abortMultipartUpload not yet implemented in storage node");
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private int getPartition(String key) {
        return Math.abs(key.hashCode() % 10);
    }
}