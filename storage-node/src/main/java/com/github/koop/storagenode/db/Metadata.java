package com.github.koop.storagenode.db;

public record Metadata(
        String location,
        String partition,
        long sequenceNumber
) {
}
