package com.github.koop.queryprocessor.processor;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.cache.CacheClient;
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Orchestrates multipart-upload lifecycle using StorageWorker + cache state.
 */
public class MultipartUploadManager {

    private final StorageWorker storageWorker;
    private final CacheClient cache;

    public MultipartUploadManager(StorageWorker storageWorker, CacheClient cache) {
        this.storageWorker = storageWorker;
        this.cache = cache;
    }

    public String initiateMultipartUpload(String bucket, String key) {
        String uploadId = UUID.randomUUID().toString();
        MultipartUploadSession session = new MultipartUploadSession(
                uploadId,
                bucket,
                key,
                MultipartUploadSession.UploadStatus.ACTIVE);

        cache.put(MultipartUploadSession.sessionKey(uploadId), session.serialize());
        return uploadId;
    }

    public String uploadPart(String bucket, String key, String uploadId,
                             int partNumber, InputStream data, long length) throws Exception {
        MultipartUploadSession session = requireSession(uploadId);
        if (session.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            throw new IllegalStateException("Upload is not ACTIVE");
        }

        String partsKey = MultipartUploadSession.partsKey(uploadId);
        String partMember = String.valueOf(partNumber);
        if (cache.setMembers(partsKey).contains(partMember)) {
            throw new IllegalStateException("Part " + partNumber + " already uploaded");
        }

        String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
        boolean stored = storageWorker.put(UUID.randomUUID(), bucket, partStorageKey, length, data);
        if (!stored) {
            throw new RuntimeException("Failed to store part " + partNumber);
        }

        cache.setAdd(partsKey, partMember);
        cache.put(MultipartUploadSession.partSizeKey(uploadId, partNumber), String.valueOf(length));
        return "";
    }

    public String completeMultipartUpload(String bucket, String key, String uploadId,
                                          List<StorageService.CompletedPart> parts) throws Exception {
        MultipartUploadSession session = requireSession(uploadId);
        if (session.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            throw new IllegalStateException("Upload is not ACTIVE");
        }

        Set<String> uploadedParts = cache.setMembers(MultipartUploadSession.partsKey(uploadId));
        for (StorageService.CompletedPart part : parts) {
            String partMember = String.valueOf(part.partNumber());
            if (!uploadedParts.contains(partMember)) {
                throw new IllegalStateException("Part " + part.partNumber() + " was not uploaded");
            }
        }

        cache.put(
                MultipartUploadSession.sessionKey(uploadId),
                session.withStatus(MultipartUploadSession.UploadStatus.COMPLETING).serialize());

        List<Integer> sortedPartNumbers = sortedUniquePartNumbers(parts);

        long totalLength = 0L;
        List<InputStream> partStreams = new ArrayList<>();
        for (int partNumber : sortedPartNumbers) {
            String sizeValue = cache.get(MultipartUploadSession.partSizeKey(uploadId, partNumber));
            if (sizeValue == null) {
                throw new IllegalStateException("Missing size for part " + partNumber);
            }
            totalLength += Long.parseLong(sizeValue);

            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            partStreams.add(storageWorker.get(UUID.randomUUID(), bucket, partStorageKey));
        }

        Enumeration<InputStream> streams = Collections.enumeration(partStreams);
        try (SequenceInputStream concatenated = new SequenceInputStream(streams)) {
            boolean stored = storageWorker.put(UUID.randomUUID(), bucket, key, totalLength, concatenated);
            if (!stored) {
                throw new RuntimeException("Failed to store completed multipart object");
            }
        } finally {
            for (InputStream stream : partStreams) {
                try {
                    stream.close();
                } catch (Exception ignored) {
                    // Best-effort cleanup; stream may already be closed by SequenceInputStream.
                }
            }
        }

        for (int partNumber : sortedPartNumbers) {
            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            storageWorker.delete(UUID.randomUUID(), bucket, partStorageKey);
            cache.delete(MultipartUploadSession.partSizeKey(uploadId, partNumber));
        }

        cache.delete(MultipartUploadSession.sessionKey(uploadId));
        cache.setDelete(MultipartUploadSession.partsKey(uploadId));

        return "";
    }

    public void abortMultipartUpload(String bucket, String key, String uploadId) throws Exception {
        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        if (serialized == null) {
            return;
        }

        MultipartUploadSession session = MultipartUploadSession.deserialize(serialized);
        cache.put(
                MultipartUploadSession.sessionKey(uploadId),
                session.withStatus(MultipartUploadSession.UploadStatus.ABORTING).serialize());

        Set<String> partMembers = cache.setMembers(MultipartUploadSession.partsKey(uploadId));
        for (String partMember : partMembers) {
            int partNumber = Integer.parseInt(partMember);
            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            storageWorker.delete(UUID.randomUUID(), bucket, partStorageKey);
            cache.delete(MultipartUploadSession.partSizeKey(uploadId, partNumber));
        }

        cache.delete(MultipartUploadSession.sessionKey(uploadId));
        cache.setDelete(MultipartUploadSession.partsKey(uploadId));
    }

    private MultipartUploadSession requireSession(String uploadId) {
        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        if (serialized == null) {
            throw new IllegalArgumentException("No such upload: " + uploadId);
        }
        return MultipartUploadSession.deserialize(serialized);
    }

    private static List<Integer> sortedUniquePartNumbers(List<StorageService.CompletedPart> parts) {
        Set<Integer> seen = new HashSet<>();
        List<Integer> ordered = new ArrayList<>();
        for (StorageService.CompletedPart part : parts) {
            int number = part.partNumber();
            if (seen.add(number)) {
                ordered.add(number);
            }
        }
        Collections.sort(ordered);
        return ordered;
    }

}
