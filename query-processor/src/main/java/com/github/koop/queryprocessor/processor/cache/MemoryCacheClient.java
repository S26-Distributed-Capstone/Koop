package com.github.koop.queryprocessor.processor.cache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link CacheClient}.
 *
 * <p>Backed by two {@link ConcurrentHashMap} instances:
 * <ul>
 *   <li>A {@code ConcurrentHashMap<String, String>} for key-value entries.</li>
 *   <li>A {@code ConcurrentHashMap<String, Set<String>>} for set entries,
 *       where each set is itself a concurrent key-set created via
 *       {@link ConcurrentHashMap#newKeySet()}.</li>
 * </ul>
 *
 * <p>A third {@code ConcurrentHashMap<String, Long>} tracks expiration times
 * for keys with TTL. Expired keys are not automatically deleted; callers should
 * check expiration before use (or implement a background cleanup task).</p>
 *
 * <p>All operations are thread-safe and suitable for use with Javalin's
 * virtual-thread executor. No external dependencies are required.
 *
 * <p>Intended for development and test scenarios. For production multi-instance
 * deployments, replace with {@code RedisCacheClient}.
 */
public class MemoryCacheClient implements CacheClient {

    private final ConcurrentHashMap<String, String> kvStore;
    private final ConcurrentHashMap<String, Set<String>> setStore;
    private final ConcurrentHashMap<String, Long> expirationTimes;

    public MemoryCacheClient() {
        this.kvStore = new ConcurrentHashMap<>();
        this.setStore = new ConcurrentHashMap<>();
        this.expirationTimes = new ConcurrentHashMap<>();
    }

    // ─── Key-Value Operations ─────────────────────────────────────────────────

    @Override
    public void put(String key, String value) {
        kvStore.put(key, value);
        expirationTimes.remove(key);
    }

    @Override
    public void putWithTTL(String key, String value, long ttlSeconds) {
        kvStore.put(key, value);
        long expirationTime = System.currentTimeMillis() + (ttlSeconds * 1000L);
        expirationTimes.put(key, expirationTime);
    }

    @Override
    public boolean putIfPresent(String key, String value) {
        // Enforce TTL validation before updating
        Long expirationTime = expirationTimes.get(key);
        if (expirationTime != null && System.currentTimeMillis() > expirationTime) {
            kvStore.remove(key);
            expirationTimes.remove(key);
            return false;
        }
        boolean updated = kvStore.computeIfPresent(key, (k, v) -> value) != null;
        if (!updated) {
            expirationTimes.remove(key);
        }
        return updated;
    }

    @Override
    public String get(String key) {
        // Check if key has expired
        Long expirationTime = expirationTimes.get(key);
        if (expirationTime != null && System.currentTimeMillis() > expirationTime) {
            kvStore.remove(key);
            expirationTimes.remove(key);
            return null;
        }
        return kvStore.get(key);
    }

    @Override
    public void delete(String key) {
        kvStore.remove(key);
        expirationTimes.remove(key);
    }

    @Override
    public boolean exists(String key) {
        // Enforce TTL validation before returning exists
        Long expirationTime = expirationTimes.get(key);
        if (expirationTime != null && System.currentTimeMillis() > expirationTime) {
            kvStore.remove(key);
            expirationTimes.remove(key);
            return false;
        }
        return kvStore.containsKey(key);
    }

    // ─── Set Operations ───────────────────────────────────────────────────────

    @Override
    public void setAdd(String key, String member) {
        // computeIfAbsent is atomic: only one Set is ever created per key
        setStore.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(member);
    }

    @Override
    public boolean setAddIfAbsent(String key, String member) {
        Set<String> set = setStore.get(key);
        if (set == null) {
            return false;
        }
        // add() returns true if member was not already present, false if already there
        return set.add(member);
    }

    @Override
    public boolean setAddIfPresent(String key, String member) {
        Set<String> existing = setStore.get(key);
        if (existing == null) {
            return false;
        }
        existing.add(member);
        return true;
    }

    @Override
    public boolean setRemove(String key, String member) {
        Set<String> existing = setStore.get(key);
        if (existing == null) {
            return false;
        }
        return existing.remove(member);
    }

    @Override
    public void setCreate(String key) {
        setStore.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
    }

    @Override
    public boolean setExists(String key) {
        return setStore.containsKey(key);
    }

    @Override
    public Set<String> setMembers(String key) {
        Set<String> members = setStore.get(key);
        if (members == null) {
            return Collections.emptySet();
        }
        // Return an unmodifiable snapshot so callers cannot mutate internal state
        return Set.copyOf(members);
    }

    @Override
    public void setDelete(String key) {
        setStore.remove(key);
    }
}
