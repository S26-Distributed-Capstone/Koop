package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record Metadata(String key, int partition, List<FileVersion> versions) {

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        List<byte[]> encodedVersions = new ArrayList<>(versions.size());
        for (FileVersion v : versions) encodedVersions.add(encodeVersion(v));

        int versionsPayload = 4; // count (int)
        for (byte[] ev : encodedVersions) versionsPayload += ev.length;

        int totalLength = 4 + keyBytes.length + 4 + versionsPayload;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(versions.size());
        for (byte[] ev : encodedVersions) buffer.put(ev);
        return buffer.array();
    }

    public static Metadata from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        String key = readString(buffer);
        int partition = buffer.getInt();
        int count = buffer.getInt();
        List<FileVersion> versions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) versions.add(decodeVersion(buffer));
        return new Metadata(key, partition, versions);
    }

    private static final byte TAG_REGULAR   = 0x00;
    private static final byte TAG_MULTIPART = 0x01;
    private static final byte TAG_TOMBSTONE = 0x02;

    private static byte[] encodeVersion(FileVersion v) {
        if (v instanceof RegularFileVersion r) {
            byte[] locBytes = r.location().getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES + 4 + locBytes.length + 1);
            buf.put(TAG_REGULAR);
            buf.putLong(r.sequenceNumber());
            buf.putInt(locBytes.length);
            buf.put(locBytes);
            buf.put((byte) (r.materialized() ? 1 : 0));
            return buf.array();
        } else if (v instanceof MultipartFileVersion m) {
            List<byte[]> chunkBytes = new ArrayList<>(m.chunks().size());
            int chunksPayload = 4;
            for (String c : m.chunks()) {
                byte[] cb = c.getBytes(StandardCharsets.UTF_8);
                chunkBytes.add(cb);
                chunksPayload += 4 + cb.length;
            }
            ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES + chunksPayload);
            buf.put(TAG_MULTIPART);
            buf.putLong(m.sequenceNumber());
            buf.putInt(m.chunks().size());
            for (byte[] cb : chunkBytes) { buf.putInt(cb.length); buf.put(cb); }
            return buf.array();
        } else if (v instanceof TombstoneFileVersion t) {
            ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES);
            buf.put(TAG_TOMBSTONE);
            buf.putLong(t.sequenceNumber());
            return buf.array();
        } else {
            throw new IllegalArgumentException("Unknown FileVersion type: " + v.getClass());
        }
    }

    private static FileVersion decodeVersion(ByteBuffer buffer) {
        byte tag = buffer.get();
        long seqNum = buffer.getLong();
        return switch (tag) {
            case TAG_REGULAR   -> {
                String loc = readString(buffer);
                boolean materialized = buffer.get() == 1;
                yield new RegularFileVersion(seqNum, loc, materialized);
            }
            case TAG_MULTIPART -> {
                int count = buffer.getInt();
                List<String> chunks = new ArrayList<>(count);
                for (int i = 0; i < count; i++) chunks.add(readString(buffer));
                yield new MultipartFileVersion(seqNum, chunks);
            }
            case TAG_TOMBSTONE -> new TombstoneFileVersion(seqNum);
            default            -> throw new IllegalArgumentException("Unknown FileVersion tag: " + tag);
        };
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}