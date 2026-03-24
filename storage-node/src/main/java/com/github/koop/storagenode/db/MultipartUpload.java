package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps an object key to an ordered list of chunk identifiers (e.g. blob paths).
 */
public record MultipartUpload(
        String key,
        List<String> chunks) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        List<byte[]> chunkBytes = new ArrayList<>(chunks.size());
        int chunksPayload = 0;
        for (String chunk : chunks) {
            byte[] b = chunk.getBytes(StandardCharsets.UTF_8);
            chunkBytes.add(b);
            chunksPayload += 4 + b.length; // length prefix + data
        }

        // Format: [keyLen][key][chunkCount][chunk0Len][chunk0]...[chunkNLen][chunkN]
        int totalLength = 4 + keyBytes.length + 4 + chunksPayload;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        writeString(buffer, keyBytes);
        buffer.putInt(chunks.size());
        for (byte[] cb : chunkBytes) {
            writeString(buffer, cb);
        }
        return buffer.array();
    }

    public static MultipartUpload from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        String key = readString(buffer);
        int chunkCount = buffer.getInt();
        List<String> chunks = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunks.add(readString(buffer));
        }
        return new MultipartUpload(key, chunks);
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
