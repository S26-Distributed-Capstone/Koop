package com.github.koop.storagenode;

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
}
