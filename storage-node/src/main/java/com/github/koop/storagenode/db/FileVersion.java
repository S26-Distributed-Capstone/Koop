package com.github.koop.storagenode.db;

/**
 * A version entry embedded in {@link Metadata}
 *
 * <p>Three permitted subtypes:
 * <ul>
 *   <li>{@link RegularFileVersion} — a single-blob version with a storage location</li>
 *   <li>{@link MultipartFileVersion} — a completed multipart version with an ordered chunk list</li>
 *   <li>{@link TombstoneFileVersion} — a logical delete marker (no location/chunks)</li>
 * </ul>
 */
public sealed interface FileVersion permits RegularFileVersion, MultipartFileVersion, TombstoneFileVersion {
    long sequenceNumber();
}
