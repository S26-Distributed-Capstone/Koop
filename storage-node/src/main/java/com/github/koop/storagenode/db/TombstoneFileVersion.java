package com.github.koop.storagenode.db;

/**
 * A tombstone version — marks a logical delete of an object.
 * Carries only the sequence number; no location or chunk data.
 */
public record TombstoneFileVersion(long sequenceNumber) implements FileVersion {}
