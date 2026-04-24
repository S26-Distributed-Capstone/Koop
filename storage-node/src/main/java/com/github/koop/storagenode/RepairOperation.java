package com.github.koop.storagenode;

/**
 * Represents a single repair task to be processed by the {@link RepairWorkerPool}.
 *
 * @param blobKey   the key of the blob that needs repair (e.g. "bucket/object")
 * @param reason    why this repair was triggered
 * @param seqOffset the pub/sub sequence offset of the message that triggered this
 *                  repair; used for at-least-once idempotency in the enqueue path.
 *                  {@link RepairReason#READ_MISS} callers should pass
 *                  {@link Long#MAX_VALUE} since they have no associated offset.
 */
public record RepairOperation(String blobKey, RepairReason reason, long seqOffset) {

    /**
     * The reason a repair operation was enqueued.
     */
    public enum RepairReason {
        /** A GET request found committed metadata but no physical file on disk. */
        READ_MISS,
        /** The node is catching up on messages missed while offline. */
        STARTUP_CATCHUP,
        /** A commit message arrived but the blob was not present on disk. */
        COMMIT_MISS
    }
}
