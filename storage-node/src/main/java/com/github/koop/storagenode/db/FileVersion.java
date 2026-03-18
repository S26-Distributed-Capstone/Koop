package com.github.koop.storagenode.db;

/**
 * A version entry embedded in {@link Metadata} (Table #2).
 *
 * <p>Two permitted subtypes:
 * <ul>
 *   <li>{@link RegularFileVersion} — a single-blob version with a storage location</li>
 *   <li>{@link MultipartFileVersion} — a completed multipart version with an ordered chunk list</li>
 * </ul>
 */
public sealed interface FileVersion permits RegularFileVersion, MultipartFileVersion {
    long sequenceNumber();
}
