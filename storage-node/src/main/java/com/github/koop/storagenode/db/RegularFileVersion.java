package com.github.koop.storagenode.db;

/**
 * A regular (single-blob) version of an object.
 * {@code location} is the blob identifier (e.g. a request/upload UUID).
 * {@code materialized} indicates whether the data payload exists on disk for this commit.
 */
public record RegularFileVersion(long sequenceNumber, String location, boolean materialized) implements FileVersion {}