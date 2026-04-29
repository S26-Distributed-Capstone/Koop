package com.github.koop.storagenode;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe tracker for object keys that are currently being written to disk.
 *
 * <p>Shared between {@link StorageNodeV2} (which registers writes) and
 * {@link RepairWorkerPool} (which checks for in-flight writes before dispatching
 * a repair), eliminating the circular dependency that previously existed between
 * those two classes.
 */
public class WriteTracker {

    private final Set<String> activeWrites = ConcurrentHashMap.newKeySet();

    /** Marks {@code key} as having an in-progress write. */
    public void begin(String key) {
        activeWrites.add(key);
    }

    /** Clears the in-progress mark for {@code key}. */
    public void end(String key) {
        activeWrites.remove(key);
    }

    /** Returns {@code true} if a write is currently in progress for {@code key}. */
    public boolean isActive(String key) {
        return activeWrites.contains(key);
    }
}
