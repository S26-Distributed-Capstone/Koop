package com.github.koop.storagenode;

/**
 * Lifecycle states for a storage node.
 *
 * <ul>
 *   <li>{@link #INITIALIZING} — Node is starting up; no traffic accepted.</li>
 *   <li>{@link #REPAIRING} — Consuming backlog messages, compacting the log,
 *       and enqueuing repair operations. HTTP traffic is blocked (503).</li>
 *   <li>{@link #ACTIVE} — Normal operations. Read-repair may still enqueue
 *       items asynchronously in the background.</li>
 * </ul>
 */
public enum NodeState {
    INITIALIZING,
    REPAIRING,
    ACTIVE
}
