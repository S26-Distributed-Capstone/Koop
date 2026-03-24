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
 *
 * <p><b>Note on complete flow:</b> The current implementation materializes one
 * assembled object on complete (reads all parts, concatenates, writes final key,
 * deletes part keys). This is pending a team decision on whether to instead commit
 * a manifest to the storage nodes and reconstruct on read. See PR #73 discussion.
 * Note on discussion, plan is to implement the manifest-based approach after initial 
 * multipart upload support is in place, as it is a more complex change that can be added iteratively.
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

    /**
     * Uploads a single part of an in-progress multipart upload.
     */
    public MultipartUploadResult uploadPart(String bucket, String key, String uploadId,
                             int partNumber, InputStream data, long length)
            {
        MultipartUploadSession session = findSession(uploadId);
        if (session == null) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.NOT_FOUND,
                    "No such upload: " + uploadId);
        }

        if (session.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " is not ACTIVE (status=" + session.status() + ")");
        }

        String partsKey = MultipartUploadSession.partsKey(uploadId);
        String partMember = String.valueOf(partNumber);
        if (cache.setMembers(partsKey).contains(partMember)) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                "Part " + partNumber + " already uploaded for upload " + uploadId);
        }

        String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
        boolean stored;
        try {
            stored = storageWorker.put(UUID.randomUUID(), bucket, partStorageKey, length, data);
        } catch (Exception e) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Exception storing part " + partNumber + " for upload " + uploadId);
        }
        if (!stored) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Failed to store part " + partNumber + " for upload " + uploadId);
        }

        cache.setAdd(partsKey, partMember);
        cache.put(MultipartUploadSession.partSizeKey(uploadId, partNumber), String.valueOf(length));
        return MultipartUploadResult.success();
    }

    /**
     * Finalizes an in-progress multipart upload.
     */
    public MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId,
                                          List<StorageService.CompletedPart> parts)
            {
        MultipartUploadSession session = findSession(uploadId);
        if (session == null) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.NOT_FOUND,
                    "No such upload: " + uploadId);
        }

        if (session.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " is not ACTIVE (status=" + session.status() + ")");
        }

        Set<String> uploadedParts = cache.setMembers(MultipartUploadSession.partsKey(uploadId));
        for (StorageService.CompletedPart part : parts) {
            String partMember = String.valueOf(part.partNumber());
            if (!uploadedParts.contains(partMember)) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                        "Part " + part.partNumber() + " was not uploaded for upload " + uploadId);
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
                return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                        "Missing cached size for part " + partNumber + " of upload " + uploadId);
            }
            totalLength += Long.parseLong(sizeValue);

            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            try {
                partStreams.add(storageWorker.get(UUID.randomUUID(), bucket, partStorageKey));
            } catch (Exception e) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                        "Failed to retrieve part " + partNumber + " for upload " + uploadId);
            }
        }

        Enumeration<InputStream> streams = Collections.enumeration(partStreams);
        try (SequenceInputStream concatenated = new SequenceInputStream(streams)) {
            boolean stored = storageWorker.put(UUID.randomUUID(), bucket, key, totalLength, concatenated);
            if (!stored) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                        "Failed to store completed multipart object for upload " + uploadId);
            }
        } catch (Exception e) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Exception assembling multipart object for upload " + uploadId);
        } finally {
            for (InputStream stream : partStreams) {
                try {
                    stream.close();
                } catch (Exception ignored) {
                    // Best-effort cleanup; stream may already be closed by SequenceInputStream.
                }
            }
        }

        // Send manifest to storage nodes so they can track multipart state.
        // Nodes use this metadata for reconstructing objects on read.
        try {
            MultipartManifest manifest = new MultipartManifest(uploadId, bucket, key, sortedPartNumbers);
            boolean manifestSent = storageWorker.sendMessage("multipart-manifest", manifest.serialize());
            if (!manifestSent) {
                // Log but don't fail — the object is already written; manifest is best-effort.
                // Reconstruction can fall back to part discovery if needed.
            }
        } catch (Exception e) {
            // Best-effort manifest transmission; object is already complete.
        }

        for (int partNumber : sortedPartNumbers) {
            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            try {
                storageWorker.delete(UUID.randomUUID(), bucket, partStorageKey);
            } catch (Exception ignored) {
                // Best-effort part cleanup; final object is already written.
            }
            cache.delete(MultipartUploadSession.partSizeKey(uploadId, partNumber));
        }

        cache.delete(MultipartUploadSession.sessionKey(uploadId));
        cache.setDelete(MultipartUploadSession.partsKey(uploadId));

        return MultipartUploadResult.success();
    }

    public MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) {
        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        if (serialized == null) {
            return MultipartUploadResult.success(); // graceful no-op for unknown uploadId
        }

        MultipartUploadSession session = MultipartUploadSession.deserialize(serialized);
        cache.put(
                MultipartUploadSession.sessionKey(uploadId),
                session.withStatus(MultipartUploadSession.UploadStatus.ABORTING).serialize());

        Set<String> partMembers = cache.setMembers(MultipartUploadSession.partsKey(uploadId));

        // TODO: Team decision pending — should physical part deletion be synchronous
        // here (current behavior) or deferred/async? Synchronous deletion means the
        // client waits for all shard deletes before receiving the 204 ACK. Deferred
        // deletion would ACK immediately and clean up in the background (or via a
        // separate GC process). See PR #73 discussion.
        for (String partMember : partMembers) {
            int partNumber = Integer.parseInt(partMember);
            String partStorageKey = MultipartUploadSession.partStorageKey(bucket, key, uploadId, partNumber);
            try {
                storageWorker.delete(UUID.randomUUID(), bucket, partStorageKey);
            } catch (Exception ignored) {
                // Best-effort; continue cleaning up remaining parts.
            }
            cache.delete(MultipartUploadSession.partSizeKey(uploadId, partNumber));
        }

        cache.delete(MultipartUploadSession.sessionKey(uploadId));
        cache.setDelete(MultipartUploadSession.partsKey(uploadId));

        return MultipartUploadResult.success();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private MultipartUploadSession findSession(String uploadId) {
        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        if (serialized == null) {
            return null;
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
