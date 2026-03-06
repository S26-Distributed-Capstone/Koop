package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record Metadata(
    String fileName,
    String location,
    String partition,
    long sequenceNumber) {

    public byte[] serialize() {
        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        byte[] locationBytes = location.getBytes(StandardCharsets.UTF_8);
        byte[] partitionBytes = partition.getBytes(StandardCharsets.UTF_8);

        int totalLength = 4 + fileNameBytes.length +
                         4 + locationBytes.length +
                         4 + partitionBytes.length +
                         8;

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        writeString(buffer, fileNameBytes);
        writeString(buffer, locationBytes);
        writeString(buffer, partitionBytes);
        buffer.putLong(sequenceNumber);

        return buffer.array();
    }

    public static Metadata from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);

        String fileName = readString(buffer);
        String location = readString(buffer);
        String partition = readString(buffer);
        long sequenceNumber = buffer.getLong();

        return new Metadata(fileName, location, partition, sequenceNumber);
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
