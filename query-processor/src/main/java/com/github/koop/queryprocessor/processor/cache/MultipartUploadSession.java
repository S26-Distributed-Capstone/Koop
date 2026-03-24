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
 * <p>Part shards are stored on storage nodes under keys derived via
 * {@link #partStorageKey(String, String, String, int)}:
 * <pre>
 * __mpu__:{bucket}:{key}:{uploadId}:{partNumber}
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
     *   <li>{@code COMPLETED}  — multipart upload successfully completed; session
     *       retained with TTL for background garbage collection to discover
     *       and clean up orphaned part shards.</li>
     * </ul>
     */
    public enum UploadStatus {
        ACTIVE,
        COMPLETING,
        ABORTING,
        COMPLETED
    }

    // ─── Serialization ────────────────────────────────────────────────────────

    private static final String DELIMITER = "|";

    /**
     * Serializes this session to a single string suitable for storage in the
     * cache key-value store.
     */
    public String serialize() {
        return escapeComponent(uploadId) + DELIMITER + escapeComponent(bucket) + DELIMITER + escapeComponent(key) + DELIMITER + status.name();
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
        String[] parts = splitSerializedComponents(value);
        if (parts.length != 4) {
            throw new IllegalArgumentException("Malformed session value: " + value);
        }
        return new MultipartUploadSession(
                unescapeComponent(parts[0]),
                unescapeComponent(parts[1]),
                unescapeComponent(parts[2]),
                UploadStatus.valueOf(parts[3])
        );
    }

    private static String[] splitSerializedComponents(String value) {
        String[] parts = new String[4];
        int partIndex = 0;
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                // Keep escape markers intact so component unescaping stays centralized.
                if (i + 1 >= value.length()) {
                    throw new IllegalArgumentException("Malformed session value (dangling escape): " + value);
                }
                current.append(c);
                current.append(value.charAt(++i));
                continue;
            }
            if (c == DELIMITER.charAt(0)) {
                if (partIndex >= 3) {
                    throw new IllegalArgumentException("Malformed session value: " + value);
                }
                parts[partIndex++] = current.toString();
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        if (partIndex != 3) {
            throw new IllegalArgumentException("Malformed session value: " + value);
        }
        parts[partIndex] = current.toString();
        return parts;
    }

    /**
     * Unescapes special characters in a component.
     */
    private static String unescapeComponent(String component) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < component.length(); i++) {
            char c = component.charAt(i);
            if (c == '\\') {
                if (i + 1 >= component.length()) {
                    throw new IllegalArgumentException("Malformed escaped component: " + component);
                }
                result.append(component.charAt(++i));
            } else {
                result.append(c);
            }
        }
        return result.toString();
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

    private static final String PART_STORAGE_PREFIX = "__mpu__:";

    /**
     * Derives the storage-node key for a specific part shard.
     * Uses a non-user-controllable namespace to prevent collisions with user keys.
     * Format: {@code __mpu__:{bucket}:{key}:{uploadId}:{partNumber}}
     */
    public static String partStorageKey(String bucket, String key, String uploadId, int partNumber) {
        return PART_STORAGE_PREFIX + escapeComponent(bucket) + ":" + escapeComponent(key) + ":" + escapeComponent(uploadId) + ":" + partNumber;
    }

    /**
     * Escapes special characters in a component to prevent key collisions.
     */
    private static String escapeComponent(String component) {
        return component
                .replace("\\", "\\\\")
                .replace(":", "\\:")
                .replace(DELIMITER, "\\" + DELIMITER);
    }

    // ─── Convenience Factory ──────────────────────────────────────────────────

    /**
     * Returns a copy of this session with the status changed to the given value.
     */
    public MultipartUploadSession withStatus(UploadStatus newStatus) {
        return new MultipartUploadSession(uploadId, bucket, key, newStatus);
    }
}
