package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.Message;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.processor.cache.CacheClient;
import com.github.koop.queryprocessor.processor.cache.MultipartUploadSession;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Orchestrates multipart-upload lifecycle using StorageWorker + cache state.
 *
 * <p><b>Completion flow:</b> Upon completion, instead of materializing the full
 * object, a manifest (MultipartCommitMessage) is sent to storage nodes listing
 * all part numbers. The individual erasure-coded parts remain in storage unchanged.
 * Reconstruction happens on read, avoiding the I/O cost of assembly on write.
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
        cache.setCreate(MultipartUploadSession.partsKey(uploadId));
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

        MultipartUploadResult mismatch = rejectSessionTargetMismatch(session, bucket, key, uploadId);
        if (mismatch != null) {
            return mismatch;
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

        String sessionBucket = session.bucket();
        String sessionKey = session.key();
        String partStorageKey = MultipartUploadSession.partStorageKey(sessionBucket, sessionKey, uploadId, partNumber);
        boolean stored;
        try {
            stored = storageWorker.put(UUID.randomUUID(), sessionBucket, partStorageKey, length, data);
        } catch (Exception e) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Exception storing part " + partNumber + " for upload " + uploadId);
        }
        if (!stored) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Failed to store part " + partNumber + " for upload " + uploadId);
        }

        if (!cache.exists(MultipartUploadSession.sessionKey(uploadId))) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " is no longer ACTIVE");
        }

        if (!cache.setAddIfPresent(partsKey, partMember)) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " was aborted");
        }

        cache.put(MultipartUploadSession.partSizeKey(uploadId, partNumber), String.valueOf(length));
        return MultipartUploadResult.success();
    }

    /**
     * Finalizes an in-progress multipart upload.
     * 
     * Instead of materializing the full object, this sends a manifest to the
     * storage nodes (via pub/sub) listing all part numbers. The parts remain
     * as individual erasure-coded shards in storage; reconstruction happens on
     * read. This avoids the I/O cost of assembly on write.
     */
    public MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId,
                                          List<StorageService.CompletedPart> parts)
            {
        MultipartUploadSession session = findSession(uploadId);
        if (session == null) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.NOT_FOUND,
                    "No such upload: " + uploadId);
        }

        MultipartUploadResult mismatch = rejectSessionTargetMismatch(session, bucket, key, uploadId);
        if (mismatch != null) {
            return mismatch;
        }

        if (session.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " is not ACTIVE (status=" + session.status() + ")");
        }

        String partsKey = MultipartUploadSession.partsKey(uploadId);
        if (!cache.setExists(partsKey)) {
            return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " was aborted");
        }

        Set<String> uploadedParts = cache.setMembers(partsKey);
        for (StorageService.CompletedPart part : parts) {
            String partMember = String.valueOf(part.partNumber());
            if (!uploadedParts.contains(partMember)) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                        "Part " + part.partNumber() + " was not uploaded for upload " + uploadId);
            }
        }

        String sessionBucket = session.bucket();
        String sessionKey = session.key();
        List<Integer> sortedPartNumbers = sortedUniquePartNumbers(parts);

        // Validate all part sizes are available (needed for potential reconstruction on read)
        for (int partNumber : sortedPartNumbers) {
            String sizeValue = cache.get(MultipartUploadSession.partSizeKey(uploadId, partNumber));
            if (sizeValue == null) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                        "Missing cached size for part " + partNumber + " of upload " + uploadId);
            }
            try {
                Long.parseLong(sizeValue);
            } catch (NumberFormatException e) {
                return MultipartUploadResult.failure(MultipartUploadResult.Status.CONFLICT,
                        "Invalid cached size for part " + partNumber + " of upload " + uploadId);
            }
        }

        if (!transitionToCompleting(uploadId)) {
            return MultipartUploadResult.failure(
                    MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " is not ACTIVE");
        }

        if (!isCompletingForTarget(uploadId, sessionBucket, sessionKey)) {
            return MultipartUploadResult.failure(
                MultipartUploadResult.Status.CONFLICT,
                "Upload " + uploadId + " was aborted");
        }

        // Send manifest to storage nodes via pub/sub (partition-keyed topic for ordering)
        List<String> partNumberStrings = new ArrayList<>();
        for (int partNum : sortedPartNumbers) {
            partNumberStrings.add(String.valueOf(partNum));
        }
        
        Message manifestMessage = new Message.MultipartCommitMessage(
            sessionBucket,
            sessionKey,
            uploadId,
            new InetSocketAddress("127.0.0.1", 0),
            partNumberStrings);
        
        boolean published;
        try {
            published = storageWorker.sendMessage(sessionBucket, sessionKey, manifestMessage);
        } catch (Exception e) {
            restoreSessionToActiveIfStillCompleting(uploadId, sessionBucket, sessionKey);
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Exception publishing multipart manifest: " + e.getMessage());
        }
        
        if (!published) {
            restoreSessionToActiveIfStillCompleting(uploadId, sessionBucket, sessionKey);
            return MultipartUploadResult.failure(MultipartUploadResult.Status.STORAGE_FAILURE,
                    "Failed to publish multipart manifest for upload " + uploadId);
        }

        // Mark upload as COMPLETED in cache and clean up session state
        cache.delete(MultipartUploadSession.sessionKey(uploadId));
        cache.setDelete(MultipartUploadSession.partsKey(uploadId));
        
        // Clean up part size metadata, but keep parts in storage for reconstruction on read
        for (int partNumber : sortedPartNumbers) {
            cache.delete(MultipartUploadSession.partSizeKey(uploadId, partNumber));
        }

        return MultipartUploadResult.success();
    }

    public MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) {
        String serialized = cache.get(MultipartUploadSession.sessionKey(uploadId));
        if (serialized == null) {
            return MultipartUploadResult.success(); // graceful no-op for unknown uploadId
        }

        MultipartUploadSession session = MultipartUploadSession.deserialize(serialized);

        MultipartUploadResult mismatch = rejectSessionTargetMismatch(session, bucket, key, uploadId);
        if (mismatch != null) {
            return mismatch;
        }

        String sessionBucket = session.bucket();
        String sessionKey = session.key();
        if (!cache.putIfPresent(
            MultipartUploadSession.sessionKey(uploadId),
            session.withStatus(MultipartUploadSession.UploadStatus.ABORTING).serialize())) {
            return MultipartUploadResult.success();
        }

        Set<String> partMembers = cache.setMembers(MultipartUploadSession.partsKey(uploadId));

        // TODO: Team decision pending — should physical part deletion be synchronous
        // here (current behavior) or deferred/async? Synchronous deletion means the
        // client waits for all shard deletes before receiving the 204 ACK. Deferred
        // deletion would ACK immediately and clean up in the background (or via a
        // separate GC process). See PR #73 discussion.
        for (String partMember : partMembers) {
            int partNumber = Integer.parseInt(partMember);
            String partStorageKey = MultipartUploadSession.partStorageKey(sessionBucket, sessionKey, uploadId, partNumber);
            try {
                storageWorker.delete(UUID.randomUUID(), sessionBucket, partStorageKey);
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

    private MultipartUploadResult rejectSessionTargetMismatch(
            MultipartUploadSession session,
            String requestBucket,
            String requestKey,
            String uploadId) {
        if (!session.bucket().equals(requestBucket) || !session.key().equals(requestKey)) {
            return MultipartUploadResult.failure(
                    MultipartUploadResult.Status.CONFLICT,
                    "Upload " + uploadId + " target mismatch");
        }
        return null;
    }

    private boolean transitionToCompleting(String uploadId) {
        MultipartUploadSession current = findSession(uploadId);
        if (current == null || current.status() != MultipartUploadSession.UploadStatus.ACTIVE) {
            return false;
        }
        return cache.putIfPresent(
                MultipartUploadSession.sessionKey(uploadId),
                current.withStatus(MultipartUploadSession.UploadStatus.COMPLETING).serialize());
    }

    private boolean isCompletingForTarget(String uploadId, String expectedBucket, String expectedKey) {
        MultipartUploadSession current = findSession(uploadId);
        if (current == null) {
            return false;
        }
        if (!current.bucket().equals(expectedBucket) || !current.key().equals(expectedKey)) {
            return false;
        }
        return current.status() == MultipartUploadSession.UploadStatus.COMPLETING;
    }

    private void restoreSessionToActiveIfStillCompleting(String uploadId, String expectedBucket, String expectedKey) {
        MultipartUploadSession current = findSession(uploadId);
        if (current == null) {
            return;
        }
        if (!current.bucket().equals(expectedBucket) || !current.key().equals(expectedKey)) {
            return;
        }
        if (current.status() != MultipartUploadSession.UploadStatus.COMPLETING) {
            return;
        }
        cache.putIfPresent(
                MultipartUploadSession.sessionKey(uploadId),
                current.withStatus(MultipartUploadSession.UploadStatus.ACTIVE).serialize());
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
