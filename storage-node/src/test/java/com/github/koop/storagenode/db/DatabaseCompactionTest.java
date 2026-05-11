package com.github.koop.storagenode.db;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatabaseCompactionTest {

    @TempDir
    Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() throws Exception {
        database = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) database.close();
    }

    @Test
    void compactRemovesVersionsStrictlyBelowWatermark() throws Exception {
        database.putItem("k", 1, 5L, "blob-5");
        database.putItem("k", 1, 10L, "blob-10");
        database.putItem("k", 1, 15L, "blob-15");

        List<FileVersion> removed = database.compactMetadata("k", 10L);

        assertEquals(1, removed.size());
        assertEquals(5L, removed.get(0).sequenceNumber());

        var versions = database.getItem("k").orElseThrow().versions();
        assertEquals(2, versions.size());
        assertEquals(10L, versions.get(0).sequenceNumber());
        assertEquals(15L, versions.get(1).sequenceNumber());
    }

    @Test
    void compactPreservesVersionsAtTheWatermark() throws Exception {
        database.putItem("k", 1, 10L, "blob-10");
        database.putItem("k", 1, 11L, "blob-11");

        List<FileVersion> removed = database.compactMetadata("k", 10L);

        assertTrue(removed.isEmpty(), "Versions equal to the watermark must be retained");
        assertEquals(2, database.getItem("k").orElseThrow().versions().size());
    }

    @Test
    void compactDeletesRowWhenAllVersionsAreBelowWatermark() throws Exception {
        database.putItem("k", 1, 5L, "blob-5");
        database.deleteItem("k", 1, 6L); // tombstone

        List<FileVersion> removed = database.compactMetadata("k", 100L);

        assertEquals(2, removed.size());
        assertTrue(database.getItem("k").isEmpty(),
                "Metadata row must be deleted when no versions survive compaction");
    }

    @Test
    void compactIsNoOpWhenWatermarkBelowAllVersions() throws Exception {
        database.putItem("k", 1, 10L, "blob-10");
        database.putItem("k", 1, 20L, "blob-20");

        List<FileVersion> removed = database.compactMetadata("k", 10L);

        assertTrue(removed.isEmpty());
        assertEquals(2, database.getItem("k").orElseThrow().versions().size());
    }

    @Test
    void compactIsNoOpForMissingKey() throws Exception {
        List<FileVersion> removed = database.compactMetadata("does-not-exist", 100L);
        assertTrue(removed.isEmpty());
    }

    @Test
    void compactReturnsRemovedMultipartVersionWithChunks() throws Exception {
        database.putMultipartItem("k", 1, 5L, List.of("c-a", "c-b", "c-c"));
        database.putItem("k", 1, 20L, "blob-20");

        List<FileVersion> removed = database.compactMetadata("k", 20L);

        assertEquals(1, removed.size());
        assertInstanceOf(MultipartFileVersion.class, removed.get(0));
        assertEquals(List.of("c-a", "c-b", "c-c"),
                ((MultipartFileVersion) removed.get(0)).chunks());
    }
}
