package com.github.koop.storagenode.db;

public record OpLog(
        String key,
        String operation
) {
}
