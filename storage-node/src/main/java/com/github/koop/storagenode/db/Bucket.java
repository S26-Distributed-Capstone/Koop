package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Table #3 — Buckets.
 * Stores bucket metadata keyed by bucket name.
 */
public record Bucket(
        String key,
        int partition,
        long sequenceNumber) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        int totalLength = 4 + keyBytes.length +
                          4 + // partition (int)
                          8;  // sequenceNumber (long)

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        writeString(buffer, keyBytes);
        buffer.putInt(partition);
        buffer.putLong(sequenceNumber);
        return buffer.array();
    }

    public static Bucket from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        String key = readString(buffer);
        int partition = buffer.getInt();
        long sequenceNumber = buffer.getLong();
        return new Bucket(key, partition, sequenceNumber);
    }

    private static void writeString(ByteBuffer buffer, byte[] bytes) {
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
