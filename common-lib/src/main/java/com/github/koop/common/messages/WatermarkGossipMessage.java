package com.github.koop.common.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Gossip payload announcing one node's local minimum active sequence numbers
 * for every partition it owns, batched into a single message.
 *
 * <p>Receivers fan the entries out into {@code PartitionWatermarks} per
 * (partition, nodeId); the global watermark for a partition is the minimum
 * over all non-stale entries.
 *
 * <p>Node-level batching (one message per node per tick rather than one per
 * partition) keeps gossip traffic O(nodes) instead of O(partitions), so the
 * protocol scales to clusters with tens of thousands of partitions.
 *
 * @param nodeId         sender identifier (typically {@code ip:port})
 * @param partitionMins  map of partition → sender's local min over
 *                       (currentPartitionSeq, lowestActiveGet)
 * @param timestampMs    wall-clock send time, used by receivers for staleness eviction
 */
public record WatermarkGossipMessage(String nodeId, Map<Integer, Long> partitionMins, long timestampMs) {

    public byte[] serialize() {
        byte[] idBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 4 + idBytes.length + Long.BYTES + 4 + partitionMins.size() * (Integer.BYTES + Long.BYTES);
        ByteBuffer buf = ByteBuffer.allocate(payloadLen);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putLong(timestampMs);
        buf.putInt(partitionMins.size());
        for (Map.Entry<Integer, Long> e : partitionMins.entrySet()) {
            buf.putInt(e.getKey());
            buf.putLong(e.getValue());
        }
        return buf.array();
    }

    public static WatermarkGossipMessage deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int idLen = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String nodeId = new String(idBytes, StandardCharsets.UTF_8);
        long timestampMs = buf.getLong();
        int count = buf.getInt();
        Map<Integer, Long> partitionMins = new HashMap<>(count * 2);
        for (int i = 0; i < count; i++) {
            int partition = buf.getInt();
            long minSeq = buf.getLong();
            partitionMins.put(partition, minSeq);
        }
        return new WatermarkGossipMessage(nodeId, partitionMins, timestampMs);
    }
}
