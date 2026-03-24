package com.github.koop.queryprocessor.processor;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.cache.MemoryCacheClient;
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultipartUploadManagerTest {

    private StorageWorker storageWorker;
    private MemoryCacheClient cache;
    private MultipartUploadManager manager;

    @BeforeEach
    void setUp() {
        storageWorker = mock(StorageWorker.class);
        cache = new MemoryCacheClient();
        manager = new MultipartUploadManager(storageWorker, cache);
    }

    @Test
    void initiate_generatesUniqueUploadId() {
        String uploadId1 = manager.initiateMultipartUpload("bucket", "key");
        String uploadId2 = manager.initiateMultipartUpload("bucket", "key");

        assertNotNull(uploadId1);
        assertNotNull(uploadId2);
        assertNotEquals(uploadId1, uploadId2);
    }

    @Test
    void initiate_storesSessionInCache() {
        String uploadId = manager.initiateMultipartUpload("bucket", "key");

        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        MultipartUploadSession session = MultipartUploadSession.deserialize(serialized);

        assertEquals(uploadId, session.uploadId());
        assertEquals("bucket", session.bucket());
        assertEquals("key", session.key());
        assertEquals(MultipartUploadSession.UploadStatus.ACTIVE, session.status());
    }

    @Test
    void uploadPart_storesPartAndUpdatesCache() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "file");
        byte[] payload = "part-1".getBytes(StandardCharsets.UTF_8);

        MultipartUploadResult result = manager.uploadPart(
                "bucket",
                "file",
                uploadId,
                1,
                new ByteArrayInputStream(payload),
                payload.length);

        assertEquals(MultipartUploadResult.Status.SUCCESS, result.status());
        assertTrue(cache.setMembers(MultipartUploadSession.partsKey(uploadId)).contains("1"));
        assertEquals(String.valueOf(payload.length), cache.get(MultipartUploadSession.partSizeKey(uploadId, 1)));

        String expectedPartKey = MultipartUploadSession.partStorageKey("bucket", "file", uploadId, 1);
        verify(storageWorker, times(1)).put(any(UUID.class), eq("bucket"), eq(expectedPartKey),
                eq((long) payload.length), any(InputStream.class));
    }

    @Test
    void uploadPart_duplicatePart_throwsConflict() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "file");
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "file", uploadId, 1, new ByteArrayInputStream(payload), payload.length);

        MultipartUploadResult result = manager.uploadPart("bucket", "file", uploadId, 1,
                new ByteArrayInputStream(payload), payload.length);
        assertEquals(MultipartUploadResult.Status.CONFLICT, result.status());
    }

    @Test
    void uploadPart_unknownUploadId_throwsNotFound() {
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);

        MultipartUploadResult result = manager.uploadPart("bucket", "file", "missing-upload", 1,
                new ByteArrayInputStream(payload), payload.length);
        assertEquals(MultipartUploadResult.Status.NOT_FOUND, result.status());
    }

    @Test
    void uploadPart_inactiveSession_throwsConflict() {
        String uploadId = "inactive-upload";
        MultipartUploadSession session = new MultipartUploadSession(
                uploadId,
                "bucket",
                "file",
                MultipartUploadSession.UploadStatus.COMPLETING);
        cache.put(MultipartUploadSession.sessionKey(uploadId), session.serialize());

        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);

        MultipartUploadResult result = manager.uploadPart("bucket", "file", uploadId, 1,
                new ByteArrayInputStream(payload), payload.length);
        assertEquals(MultipartUploadResult.Status.CONFLICT, result.status());
    }

    @Test
    void complete_allPartsPresent_assemblesAndStores() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "final-key");
        byte[] p1 = "hello ".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "world".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "final-key", uploadId, 1, new ByteArrayInputStream(p1), p1.length);
        manager.uploadPart("bucket", "final-key", uploadId, 2, new ByteArrayInputStream(p2), p2.length);

        String part1Key = MultipartUploadSession.partStorageKey("bucket", "final-key", uploadId, 1);
        String part2Key = MultipartUploadSession.partStorageKey("bucket", "final-key", uploadId, 2);

        when(storageWorker.get(any(UUID.class), eq("bucket"), eq(part1Key)))
                .thenReturn(new ByteArrayInputStream(p1));
        when(storageWorker.get(any(UUID.class), eq("bucket"), eq(part2Key)))
                .thenReturn(new ByteArrayInputStream(p2));

        List<StorageService.CompletedPart> parts = List.of(
                new StorageService.CompletedPart(1),
                new StorageService.CompletedPart(2));

        MultipartUploadResult result = manager.completeMultipartUpload("bucket", "final-key", uploadId, parts);

        assertEquals(MultipartUploadResult.Status.SUCCESS, result.status());
        verify(storageWorker, times(1)).put(any(UUID.class), eq("bucket"), eq("final-key"),
                eq((long) p1.length + p2.length), any(InputStream.class));
        verify(storageWorker, times(1)).delete(any(UUID.class), eq("bucket"), eq(part1Key));
        verify(storageWorker, times(1)).delete(any(UUID.class), eq("bucket"), eq(part2Key));
    }

    @Test
    void complete_missingPart_throwsConflict() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "final-key");
        byte[] p1 = "hello".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "final-key", uploadId, 1, new ByteArrayInputStream(p1), p1.length);

        List<StorageService.CompletedPart> parts = List.of(
                new StorageService.CompletedPart(1),
                new StorageService.CompletedPart(2));

        MultipartUploadResult result = manager.completeMultipartUpload("bucket", "final-key", uploadId, parts);
        assertEquals(MultipartUploadResult.Status.CONFLICT, result.status());
    }

    @Test
    void complete_cleansUpCacheAfterSuccess() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "final-key");
        byte[] p1 = "a".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "final-key", uploadId, 1, new ByteArrayInputStream(p1), p1.length);

        String part1Key = MultipartUploadSession.partStorageKey("bucket", "final-key", uploadId, 1);
        when(storageWorker.get(any(UUID.class), eq("bucket"), eq(part1Key)))
                .thenReturn(new ByteArrayInputStream(p1));

        manager.completeMultipartUpload(
                "bucket",
                "final-key",
                uploadId,
                List.of(new StorageService.CompletedPart(1)));

        assertEquals(null, cache.get(MultipartUploadSession.sessionKey(uploadId)));
        assertTrue(cache.setMembers(MultipartUploadSession.partsKey(uploadId)).isEmpty());
        assertEquals(null, cache.get(MultipartUploadSession.partSizeKey(uploadId, 1)));
    }

    @Test
    void abort_marksAsAborting_thenCleansUp() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "file");
        byte[] payload = "part".getBytes(StandardCharsets.UTF_8);

        manager.uploadPart("bucket", "file", uploadId, 1, new ByteArrayInputStream(payload), payload.length);
        String partKey = MultipartUploadSession.partStorageKey("bucket", "file", uploadId, 1);

        manager.abortMultipartUpload("bucket", "file", uploadId);

        verify(storageWorker, times(1)).delete(any(UUID.class), eq("bucket"), eq(partKey));
        assertEquals(null, cache.get(MultipartUploadSession.sessionKey(uploadId)));
        assertTrue(cache.setMembers(MultipartUploadSession.partsKey(uploadId)).isEmpty());
        assertEquals(null, cache.get(MultipartUploadSession.partSizeKey(uploadId, 1)));
    }

    @Test
    void abort_unknownUploadId_isNoOp() throws Exception {
        manager.abortMultipartUpload("bucket", "file", "missing-upload");
        verify(storageWorker, never()).delete(any(UUID.class), anyString(), anyString());
    }

    @Test
    void concurrent_twoPartsUploadedSimultaneously() throws Exception {
        when(storageWorker.put(any(UUID.class), anyString(), anyString(), anyLong(), any(InputStream.class)))
                .thenReturn(true);

        String uploadId = manager.initiateMultipartUpload("bucket", "file");

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Callable<MultipartUploadResult> task1 = () -> manager.uploadPart(
                    "bucket",
                    "file",
                    uploadId,
                    1,
                    new ByteArrayInputStream("part1".getBytes(StandardCharsets.UTF_8)),
                    5);
            Callable<MultipartUploadResult> task2 = () -> manager.uploadPart(
                    "bucket",
                    "file",
                    uploadId,
                    2,
                    new ByteArrayInputStream("part2".getBytes(StandardCharsets.UTF_8)),
                    5);

            executor.invokeAll(List.of(task1, task2));
        }

        Set<String> members = cache.setMembers(MultipartUploadSession.partsKey(uploadId));
        assertEquals(Set.of("1", "2"), members);
    }
}
