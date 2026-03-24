package com.github.koop.queryprocessor.processor;

public record MultipartUploadResult(Status status, String message) {

    public enum Status {
        SUCCESS,
        NOT_FOUND,
        CONFLICT,
        STORAGE_FAILURE
    }

    public static MultipartUploadResult success() {
        return new MultipartUploadResult(Status.SUCCESS, "");
    }

    public static MultipartUploadResult failure(Status status, String message) {
        return new MultipartUploadResult(status, message);
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
}
