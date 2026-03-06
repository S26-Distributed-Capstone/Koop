package com.github.koop.storagenode.db;

public record Metadata(
        String fileName,
        String location,
        String partition,
        long sequenceNumber) {
}
