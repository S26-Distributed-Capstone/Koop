package com.github.koop.common.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Gossip payload announcing one node's local minimum active sequence number
 * for a single partition.
 *
 * <p>Receivers aggregate these per (partition, nodeId) and compute the global
 * watermark as the minimum across all non-stale entries.
 *
 * @param nodeId       sender identifier (typically {@code ip:port})
 * @param partition    partition this announcement applies to
 * @param minSeqNum    sender's local min over (currentPartitionSeq, lowestActiveGet)
 * @param timestampMs  wall-clock send time, used by receivers for staleness eviction
 */
public record WatermarkGossipMessage(String nodeId, int partition, long minSeqNum, long timestampMs) {

    public byte[] serialize() {
        byte[] idBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 4 + Long.BYTES + Long.BYTES);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putInt(partition);
        buf.putLong(minSeqNum);
        buf.putLong(timestampMs);
        return buf.array();
    }

    public static WatermarkGossipMessage deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int idLen = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String nodeId = new String(idBytes, StandardCharsets.UTF_8);
        int partition = buf.getInt();
        long minSeqNum = buf.getLong();
        long timestampMs = buf.getLong();
        return new WatermarkGossipMessage(nodeId, partition, minSeqNum, timestampMs);
    }
}
