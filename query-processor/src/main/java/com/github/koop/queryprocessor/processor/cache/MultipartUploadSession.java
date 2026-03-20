package com.github.koop.queryprocessor.processor.cache;

/**
 * Immutable value type that captures the state of a single multipart upload.
 *
 * <p>Instances are serialized to/from the {@link CacheClient} using the
 * following cache-key scheme:
 *
 * <pre>
 * Session metadata  →  mpu:session:{uploadId}
 * Uploaded parts    →  mpu:parts:{uploadId}        (set of part-number strings)
 * </pre>
 *
 * <p>Part shards are stored on storage nodes under the derived key:
 * <pre>
 * {bucket}-{key}-mpu-{uploadId}-part-{partNumber}
 * </pre>
 *
 * <p>Serialization format (stored as a single cache string):
 * <pre>
 * {uploadId}|{bucket}|{key}|{status}
 * </pre>
 * The uploaded-parts set is tracked separately in the cache under
 * {@code mpu:parts:{uploadId}} so that individual part additions are atomic
 * set operations rather than requiring a read-modify-write on the session
 * string.
 */
public record MultipartUploadSession(
        String uploadId,
        String bucket,
        String key,
        MultipartUploadSession.UploadStatus status
) {

    /**
     * Lifecycle status of a multipart upload session.
     *
     * <ul>
     *   <li>{@code ACTIVE}     — upload is in progress; parts may be uploaded.</li>
     *   <li>{@code COMPLETING} — {@code CompleteMultipartUpload} is in flight;
     *       no further parts should be accepted.</li>
     *   <li>{@code ABORTING}   — {@code AbortMultipartUpload} has been acknowledged
     *       to the client; cleanup of part shards is in progress.</li>
     * </ul>
     */
    public enum UploadStatus {
        ACTIVE,
        COMPLETING,
        ABORTING
    }

    // ─── Serialization ────────────────────────────────────────────────────────

    private static final String DELIMITER = "|";
    private static final String DELIMITER_REGEX = "\\|";

    /**
     * Serializes this session to a single string suitable for storage in the
     * cache key-value store.
     */
    public String serialize() {
        return uploadId + DELIMITER + bucket + DELIMITER + key + DELIMITER + status.name();
    }

    /**
     * Deserializes a session from the string produced by {@link #serialize()}.
     *
     * @throws IllegalArgumentException if {@code value} is malformed.
     */
    public static MultipartUploadSession deserialize(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot deserialize null session value");
        }
        String[] parts = value.split(DELIMITER_REGEX, 4);
        if (parts.length != 4) {
            throw new IllegalArgumentException("Malformed session value: " + value);
        }
        return new MultipartUploadSession(
                parts[0],
                parts[1],
                parts[2],
                UploadStatus.valueOf(parts[3])
        );
    }

    // ─── Cache Key Helpers ────────────────────────────────────────────────────

    /** Cache key for the session metadata entry. */
    public static String sessionKey(String uploadId) {
        return "mpu:session:" + uploadId;
    }

    /** Cache key for the set of uploaded part numbers. */
    public static String partsKey(String uploadId) {
        return "mpu:parts:" + uploadId;
    }

    /** Cache key for a specific part size entry. */
    public static String partSizeKey(String uploadId, int partNumber) {
        return "mpu:partsize:" + uploadId + ":" + partNumber;
    }

    // ─── Storage Key Helper ───────────────────────────────────────────────────

    /**
     * Derives the storage-node key for a specific part shard.
     * Format: {@code {bucket}-{key}-mpu-{uploadId}-part-{partNumber}}
     */
    public static String partStorageKey(String bucket, String key, String uploadId, int partNumber) {
        return bucket + "-" + key + "-mpu-" + uploadId + "-part-" + partNumber;
    }

    // ─── Convenience Factory ──────────────────────────────────────────────────

    /**
     * Returns a copy of this session with the status changed to the given value.
     */
    public MultipartUploadSession withStatus(UploadStatus newStatus) {
        return new MultipartUploadSession(uploadId, bucket, key, newStatus);
    }
}
