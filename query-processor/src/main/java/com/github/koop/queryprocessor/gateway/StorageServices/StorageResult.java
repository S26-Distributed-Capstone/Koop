package com.github.koop.queryprocessor.gateway.StorageServices;

/**
 * Result of a storage mutation that may fail in expected ways (e.g. quorum miss).
 *
 * Used in place of throwing {@link RuntimeException} for routine failures so the
 * gateway can map them to appropriate S3 error responses without paying the cost
 * of stack-trace allocation and without masking real bugs caught by a generic
 * {@code catch (Exception e)}.
 */
public sealed interface StorageResult {

    record Success() implements StorageResult {}

    record Failure(String code, String message, int httpStatus) implements StorageResult {}

    StorageResult SUCCESS = new Success();

    static StorageResult success() {
        return SUCCESS;
    }

    static StorageResult failure(String code, String message, int httpStatus) {
        return new Failure(code, message, httpStatus);
    }
}
