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
 * <p>All operations are thread-safe and suitable for use with Javalin's
 * virtual-thread executor. No external dependencies are required.
 *
 * <p>Intended for development and test scenarios. For production multi-instance
 * deployments, replace with {@code RedisCacheClient}.
 */
public class MemoryCacheClient implements CacheClient {

    private final ConcurrentHashMap<String, String> kvStore;
    private final ConcurrentHashMap<String, Set<String>> setStore;

    public MemoryCacheClient() {
        this.kvStore = new ConcurrentHashMap<>();
        this.setStore = new ConcurrentHashMap<>();
    }

    // ─── Key-Value Operations ─────────────────────────────────────────────────

    @Override
    public void put(String key, String value) {
        kvStore.put(key, value);
    }

    @Override
    public boolean putIfPresent(String key, String value) {
        return kvStore.computeIfPresent(key, (k, v) -> value) != null;
    }

    @Override
    public String get(String key) {
        return kvStore.get(key);
    }

    @Override
    public void delete(String key) {
        kvStore.remove(key);
    }

    @Override
    public boolean exists(String key) {
        return kvStore.containsKey(key);
    }

    // ─── Set Operations ───────────────────────────────────────────────────────

    @Override
    public void setAdd(String key, String member) {
        // computeIfAbsent is atomic: only one Set is ever created per key
        setStore.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(member);
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
