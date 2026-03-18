package com.github.koop.storagenode.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Table #2 — Metadata.
 *
 * <p>Each row tracks a key's full history of committed versions:
 * <ul>
 *   <li>{@code key}       — the object key (e.g. "animals/cat.jpg")</li>
 *   <li>{@code partition} — the partition this key lives on</li>
 *   <li>{@code versions}  — ordered list of {@link FileVersion} entries (latest is last).
 *                           Each entry is either a {@link RegularFileVersion} or a
 *                           {@link MultipartFileVersion}.</li>
 * </ul>
 *
 * <p>Binary format:
 * <pre>
 *   [keyLen (int)][key]
 *   [partition (int)]
 *   [versionCount (int)]
 *   for each version:
 *     [type (byte): 0x00=Regular, 0x01=Multipart]
 *     [seqNum (long)]
 *     if Regular:   [locationLen (int)][location]
 *     if Multipart: [chunkCount (int)] ([chunkLen (int)][chunk]) * chunkCount
 * </pre>
 */
public record Metadata(
        String key,
        int partition,
        List<FileVersion> versions) {

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        // Pre-encode each version to calculate total size
        List<byte[]> encodedVersions = new ArrayList<>(versions.size());
        for (FileVersion v : versions) {
            encodedVersions.add(encodeVersion(v));
        }

        int versionsPayload = 4; // count (int)
        for (byte[] ev : encodedVersions) {
            versionsPayload += ev.length;
        }

        int totalLength = 4 + keyBytes.length + 4 + versionsPayload;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(partition);
        buffer.putInt(versions.size());
        for (byte[] ev : encodedVersions) {
            buffer.put(ev);
        }

        return buffer.array();
    }

    public static Metadata from(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);

        String key = readString(buffer);
        int partition = buffer.getInt();
        int versionCount = buffer.getInt();

        List<FileVersion> versions = new ArrayList<>(versionCount);
        for (int i = 0; i < versionCount; i++) {
            versions.add(decodeVersion(buffer));
        }

        return new Metadata(key, partition, versions);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static final byte TAG_REGULAR   = 0x00;
    private static final byte TAG_MULTIPART = 0x01;

    private static byte[] encodeVersion(FileVersion v) {
        if (v instanceof RegularFileVersion r) {
            byte[] locBytes = r.location().getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES + 4 + locBytes.length);
            buf.put(TAG_REGULAR);
            buf.putLong(r.sequenceNumber());
            buf.putInt(locBytes.length);
            buf.put(locBytes);
            return buf.array();
        } else if (v instanceof MultipartFileVersion m) {
            List<byte[]> chunkBytes = new ArrayList<>(m.chunks().size());
            int chunksPayload = 4; // chunk count (int)
            for (String chunk : m.chunks()) {
                byte[] cb = chunk.getBytes(StandardCharsets.UTF_8);
                chunkBytes.add(cb);
                chunksPayload += 4 + cb.length;
            }
            ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES + chunksPayload);
            buf.put(TAG_MULTIPART);
            buf.putLong(m.sequenceNumber());
            buf.putInt(m.chunks().size());
            for (byte[] cb : chunkBytes) {
                buf.putInt(cb.length);
                buf.put(cb);
            }
            return buf.array();
        } else {
            throw new IllegalArgumentException("Unknown FileVersion type: " + v.getClass());
        }
    }

    private static FileVersion decodeVersion(ByteBuffer buffer) {
        byte tag = buffer.get();
        long seqNum = buffer.getLong();
        if (tag == TAG_REGULAR) {
            return new RegularFileVersion(seqNum, readString(buffer));
        } else if (tag == TAG_MULTIPART) {
            int chunkCount = buffer.getInt();
            List<String> chunks = new ArrayList<>(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                chunks.add(readString(buffer));
            }
            return new MultipartFileVersion(seqNum, chunks);
        } else {
            throw new IllegalArgumentException("Unknown FileVersion tag: " + tag);
        }
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
