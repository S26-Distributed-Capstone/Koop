package com.github.koop.queryprocessor.processor;

import java.io.IOException;

/**
 * Thrown when a GET request targets an object whose latest version is a
 * tombstone (i.e., the object was deleted).
 *
 * <p>This typed exception allows the API gateway to distinguish "object was
 * deleted" (→ 404 NoSuchKey) from genuine storage errors (→ 500 InternalError).
 */
public class ObjectDeletedException extends IOException {

    public ObjectDeletedException(String storageKey) {
        super("Object not found (deleted): " + storageKey);
    }
}
