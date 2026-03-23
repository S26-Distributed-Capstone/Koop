package com.github.koop.storagenode.db;

import java.util.List;

/**
 * A completed multipart-upload version of an object.
 * {@code chunks} is the ordered list of blob/chunk identifiers that make up this version.
 */
public record MultipartFileVersion(long sequenceNumber, List<String> chunks) implements FileVersion {}
