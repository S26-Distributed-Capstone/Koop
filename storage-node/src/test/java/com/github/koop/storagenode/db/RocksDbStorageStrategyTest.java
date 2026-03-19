package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RocksDbStorageStrategyTest {

    @TempDir
    Path tempDir;

    private RocksDbStorageStrategy strategy;

    @BeforeEach
    void setUp() throws Exception {
        strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (strategy != null) strategy.close();
    }

    // =========================================================================
    // Table #1 — Op Log
    // =========================================================================

    @Test
    void testAddLogAndGetLogsRange() throws Exception {
        for (long i = 1; i <= 5; i++) {
            strategy.addLog(new OpLog(i, "file_" + i, Operation.PUT));
        }
        try (Stream<OpLog> stream = strategy.getLogs(2L, 4L)) {
            List<OpLog> logs = stream.collect(Collectors.toList());
            assertEquals(3, logs.size());
            assertEquals(4L, logs.get(0).seqNum());
            assertEquals(3L, logs.get(1).seqNum());
            assertEquals(2L, logs.get(2).seqNum());
        }
    }

    // =========================================================================
    // Table #2 — Metadata with FileVersion hierarchy
    // =========================================================================

    @Test
    void testUpdateAndGetMetadataRegularVersion() throws Exception {
        Metadata meta = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "/data/cat.blob")));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("animals/cat.jpg").orElseThrow();
        assertEquals(1, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertEquals(100L, retrieved.versions().get(0).sequenceNumber());
        assertEquals("/data/cat.blob", ((RegularFileVersion) retrieved.versions().get(0)).location());
    }

    @Test
    void testUpdateAndGetMetadataMultipartVersion() throws Exception {
        Metadata meta = new Metadata("animals/dog.jpg", 1,
                List.of(new MultipartFileVersion(98L, List.of("chunk-0.blob", "chunk-1.blob"))));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("animals/dog.jpg").orElseThrow();
        assertInstanceOf(MultipartFileVersion.class, retrieved.versions().get(0));
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"),
                ((MultipartFileVersion) retrieved.versions().get(0)).chunks());
    }

    @Test
    void testUpdateAndGetMetadataMixedVersions() throws Exception {
        Metadata meta = new Metadata("mixed/key", 2, List.of(
                new RegularFileVersion(10L, "/blob-10"),
                new MultipartFileVersion(20L, List.of("chunk-a")),
                new RegularFileVersion(30L, "/blob-30")));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("mixed/key").orElseThrow();
        assertEquals(3, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, retrieved.versions().get(1));
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(2));
    }

    @Test
    void testAtomicallyUpdateLogAndMetadata() throws Exception {
        long seq = 42L;
        String key = "atomic.txt";
        strategy.atomicallyUpdateLogAndMetadata(
                new OpLog(seq, key, Operation.DELETE),
                new Metadata(key, 2, List.of(new RegularFileVersion(seq, Database.TOMBSTONE_LOCATION))));

        assertEquals(Database.TOMBSTONE_LOCATION,
                ((RegularFileVersion) strategy.getMetadata(key).orElseThrow().versions().get(0)).location());

        try (Stream<OpLog> s = strategy.getLogs(seq, seq)) {
            assertEquals(Operation.DELETE, s.findFirst().orElseThrow().operation());
        }
    }

    @Test
    void testGetMetadataReturnsEmptyForMissingKey() throws Exception {
        assertTrue(strategy.getMetadata("missing").isEmpty());
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        strategy.updateMetadata(new Metadata("photos/cat.jpg", 1, List.of(new RegularFileVersion(1L, "/l1"))));
        strategy.updateMetadata(new Metadata("photos/dog.jpg", 1, List.of(new RegularFileVersion(2L, "/l2"))));
        strategy.updateMetadata(new Metadata("videos/clip.mp4", 1, List.of(new RegularFileVersion(3L, "/l3"))));

        try (Stream<Metadata> stream = strategy.streamMetadataWithPrefix("photos/")) {
            List<String> keys = stream.map(Metadata::key).collect(Collectors.toList());
            assertEquals(2, keys.size());
            assertTrue(keys.containsAll(List.of("photos/cat.jpg", "photos/dog.jpg")));
        }
    }

    // =========================================================================
    // Table #3 — Buckets (with tombstone support)
    // =========================================================================

    @Test
    void testCreateAndCheckBucketExists() throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(5L, "animals", Operation.CREATE_BUCKET),
                new Bucket("animals", 1, 5L, false));

        var bucket = strategy.getBucket("animals").orElseThrow();
        assertFalse(bucket.deleted());
        assertEquals(5L, bucket.sequenceNumber());
    }

    @Test
    void testDeleteBucketTombstone() throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(5L, "animals", Operation.CREATE_BUCKET),
                new Bucket("animals", 1, 5L, false));
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(10L, "animals", Operation.DELETE_BUCKET),
                new Bucket("animals", 1, 10L, true));

        var bucket = strategy.getBucket("animals").orElseThrow();
        assertTrue(bucket.deleted());
    }

    @Test
    void testBucketSerializationRoundTrip() {
        Bucket original = new Bucket("animals", 3, 42L, false);
        Bucket rt = Bucket.from(original.serialize());
        assertEquals("animals", rt.key());
        assertEquals(3, rt.partition());
        assertEquals(42L, rt.sequenceNumber());
        assertFalse(rt.deleted());

        Bucket tombstone = new Bucket("animals", 3, 99L, true);
        Bucket rtTombstone = Bucket.from(tombstone.serialize());
        assertTrue(rtTombstone.deleted());
    }

    @Test
    void testGetBucketReturnsEmptyForMissingKey() throws Exception {
        assertTrue(strategy.getBucket("nonexistent").isEmpty());
    }
}