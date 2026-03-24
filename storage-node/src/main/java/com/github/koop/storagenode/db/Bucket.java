package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Table #3 — Buckets.
 * {@code deleted = true} marks a tombstone (logically deleted, pending GC).
 */
public record Bucket(
        String key,
        int partition,
        long sequenceNumber,
        boolean deleted) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int totalLength = 4 + keyBytes.length + 4 + 8 + 1; // key + partition + seqNum + deleted flag
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(partition);
        buffer.putLong(sequenceNumber);
        buffer.put(deleted ? (byte) 1 : (byte) 0);
        return buffer.array();
    }

    public static Bucket from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        int len = buffer.getInt();
        byte[] keyBytes = new byte[len];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        int partition = buffer.getInt();
        long sequenceNumber = buffer.getLong();
        boolean deleted = buffer.get() == 1;
        return new Bucket(key, partition, sequenceNumber, deleted);
    }
}
