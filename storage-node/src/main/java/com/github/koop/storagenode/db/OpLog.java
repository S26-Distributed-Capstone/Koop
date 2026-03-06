package com.github.koop.storagenode.db;

public record OpLog(
        long seqNum,
        String key,
        String operation
) {
}
