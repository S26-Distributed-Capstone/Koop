package com.github.koop.queryprocessor.gateway.cache;

import java.util.Set;

/**
 * Strategy interface for the multipart-upload cache layer.
 *
 * Mirrors the {@code Fetcher} strategy pattern from {@code common-lib}: callers
 * depend only on this interface; the concrete implementation (in-memory for
 * dev/test, Redis for production) is injected at construction time.
 *
 * <p>Two logical data structures are exposed:
 * <ul>
 *   <li><b>Key-value store</b> — arbitrary string → string mappings used to
 *       persist session metadata (upload ID, bucket, key, status).</li>
 *   <li><b>Set store</b> — string key → {@code Set<String>} used to track
 *       which part numbers have been successfully uploaded for a given
 *       multipart upload session.</li>
 * </ul>
 *
 * <p>All implementations must be safe for concurrent access from multiple
 * virtual threads (Javalin's threading model).
 */
public interface CacheClient {

    // ─── Key-Value Operations ─────────────────────────────────────────────────

    /**
     * Stores {@code value} under {@code key}, overwriting any existing value.
     */
    void put(String key, String value);

    /**
     * Returns the value stored under {@code key}, or {@code null} if absent.
     */
    String get(String key);

    /**
     * Removes the entry for {@code key}. No-op if the key does not exist.
     */
    void delete(String key);

    /**
     * Returns {@code true} if {@code key} has an associated value.
     */
    boolean exists(String key);

    // ─── Set Operations ───────────────────────────────────────────────────────

    /**
     * Adds {@code member} to the set identified by {@code key}.
     * Creates the set if it does not yet exist.
     */
    void setAdd(String key, String member);

    /**
     * Returns all members of the set identified by {@code key}.
     * Returns an empty set if the key does not exist.
     */
    Set<String> setMembers(String key);

    /**
     * Removes the entire set identified by {@code key}.
     * No-op if the key does not exist.
     */
    void setDelete(String key);
}
