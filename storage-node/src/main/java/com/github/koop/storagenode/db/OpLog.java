package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record OpLog(
        long seqNum,
        String key,
        Operation operation) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] opBytes = operation.name().getBytes(StandardCharsets.UTF_8);

        int totalLen = Long.BYTES + 4 + keyBytes.length + 4 + opBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putLong(seqNum);
        writeString(buf, keyBytes);
        writeString(buf, opBytes);
        return buf.array();
    }

    public static OpLog from(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        long seqNum = buf.getLong();
        String key = readString(buf);
        String operation = readString(buf);

        return new OpLog(seqNum, key, Operation.fromString(operation));
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
