package com.github.koop.storagenode.db;

/**
 * A regular (single-blob) version of an object.
 * {@code location} is the blob identifier (e.g. a request/upload UUID).
 */
public record RegularFileVersion(long sequenceNumber, String location) implements FileVersion {}
