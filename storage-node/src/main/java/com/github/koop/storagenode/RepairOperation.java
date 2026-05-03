package com.github.koop.storagenode;

import java.io.*;

/**
 * Represents a single repair task to be processed by the {@link RepairWorkerPool}.
 *
 * @param blobKey   the key of the blob that needs repair (e.g. "bucket/object")
 * @param seqOffset the pub/sub sequence offset of the commit message that
 *                  triggered this repair; used for last-writer-wins compaction
 *                  in the enqueue path.
 * @param requestId the request ID from the commit message; used as the storage
 *                  location when writing the recovered shard to disk.
 */
public record RepairOperation(String blobKey, long seqOffset, String requestId) {

    public byte[] serialize() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(blobKey);
            dos.writeLong(seqOffset);
            dos.writeBoolean(requestId != null);
            if (requestId != null) {
                dos.writeUTF(requestId);
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static RepairOperation deserialize(byte[] data) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            String blobKey = dis.readUTF();
            long seqOffset = dis.readLong();
            String requestId = dis.readBoolean() ? dis.readUTF() : null;
            return new RepairOperation(blobKey, seqOffset, requestId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
