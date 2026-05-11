package com.github.koop.storagenode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Wire format for the GC gossip protocol.
 *
 * <p>Each storage node periodically broadcasts a {@code GossipMessage}
 * advertising the lowest in-use sequence number per partition (see
 * {@link ActiveSequenceTracker}). Peers compute the per-partition global
 * minimum and use it as the safe-deletion watermark.
 *
 * <p>Binary layout:
 * <pre>
 *   int  nodeIdLen
 *   byte nodeId[nodeIdLen]
 *   int  numPartitions
 *   { int partition, long seqNum } * numPartitions
 * </pre>
 */
public record GossipMessage(String nodeId, Map<Integer, Long> lowestInUsePerPartition) {

    public byte[] serialize() {
        byte[] nodeBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        int size = 4 + nodeBytes.length + 4 + lowestInUsePerPartition.size() * (4 + 8);
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(nodeBytes.length);
        buf.put(nodeBytes);
        buf.putInt(lowestInUsePerPartition.size());
        for (Map.Entry<Integer, Long> entry : lowestInUsePerPartition.entrySet()) {
            buf.putInt(entry.getKey());
            buf.putLong(entry.getValue());
        }
        return buf.array();
    }

    public static GossipMessage deserialize(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int nodeLen = buf.getInt();
        byte[] nodeBytes = new byte[nodeLen];
        buf.get(nodeBytes);
        String nodeId = new String(nodeBytes, StandardCharsets.UTF_8);
        int n = buf.getInt();
        Map<Integer, Long> map = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            int partition = buf.getInt();
            long seq = buf.getLong();
            map.put(partition, seq);
        }
        return new GossipMessage(nodeId, map);
    }
}
