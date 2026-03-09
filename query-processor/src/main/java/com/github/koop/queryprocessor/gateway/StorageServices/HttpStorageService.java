package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * HTTP-based client for the Storage Node.
 *
 * Replaces the old TCP binary protocol with simple HTTP calls.
 * The storage node exposes:
 *   PUT    /store/{partition}/{key}?requestId=...
 *   GET    /store/{partition}/{key}
 *   DELETE /store/{partition}/{key}
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
            return null; // Not found
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

    private int getPartition(String key) {
        return Math.abs(key.hashCode() % 10);
    }
}