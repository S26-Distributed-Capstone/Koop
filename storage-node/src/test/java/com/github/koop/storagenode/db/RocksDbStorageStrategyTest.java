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
        int partition = 1;
        for (long i = 1; i <= 5; i++) {
            strategy.addLog(new OpLog(partition, i, "file_" + i, Operation.PUT));
        }
        
        // Iterating backwards from 4L down to 2L
        try (Stream<OpLog> stream = strategy.getLogs(partition, 4L, 2L)) {
            List<OpLog> logs = stream.collect(Collectors.toList());
            assertEquals(3, logs.size());
            assertEquals(4L, logs.get(0).seqNum());
            assertEquals(3L, logs.get(1).seqNum());
            assertEquals(2L, logs.get(2).seqNum());
        }
    }

    @Test
    void testGetLogsAcrossMultiplePartitions() throws Exception {
        strategy.addLog(new OpLog(1, 1L, "file_a", Operation.PUT));
        strategy.addLog(new OpLog(2, 1L, "file_b", Operation.PUT));
        strategy.addLog(new OpLog(1, 2L, "file_c", Operation.PUT));
        strategy.addLog(new OpLog(2, 2L, "file_d", Operation.PUT));

        try (Stream<OpLog> stream = strategy.getLogs(1, 2L, 1L)) {
            List<OpLog> logs = stream.collect(Collectors.toList());
            assertEquals(2, logs.size());
            assertEquals("file_c", logs.get(0).key());
            assertEquals("file_a", logs.get(1).key());
        }

        try (Stream<OpLog> stream = strategy.getLogs(2, 2L, 1L)) {
            List<OpLog> logs = stream.collect(Collectors.toList());
            assertEquals(2, logs.size());
            assertEquals("file_d", logs.get(0).key());
            assertEquals("file_b", logs.get(1).key());
        }
    }

    // =========================================================================
    // Table #2 — Metadata with all three FileVersion types
    // =========================================================================

    @Test
    void testUpdateAndGetMetadataRegularVersion() throws Exception {
        Metadata meta = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "/data/cat.blob")));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("animals/cat.jpg").orElseThrow();
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
    void testUpdateAndGetMetadataTombstoneVersion() throws Exception {
        Metadata meta = new Metadata("animals/cat.jpg", 1, List.of(
                new RegularFileVersion(100L, "/data/cat.blob"),
                new TombstoneFileVersion(101L)));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("animals/cat.jpg").orElseThrow();
        assertEquals(2, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertInstanceOf(TombstoneFileVersion.class, retrieved.versions().get(1));
        assertEquals(101L, retrieved.versions().get(1).sequenceNumber());
    }

    @Test
    void testUpdateAndGetMetadataAllThreeTypesRoundTrip() throws Exception {
        Metadata meta = new Metadata("mixed/key", 2, List.of(
                new RegularFileVersion(10L, "/blob-10"),
                new MultipartFileVersion(20L, List.of("chunk-a")),
                new TombstoneFileVersion(30L)));
        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata("mixed/key").orElseThrow();
        assertEquals(3, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, retrieved.versions().get(1));
        assertInstanceOf(TombstoneFileVersion.class, retrieved.versions().get(2));
        assertEquals(30L, retrieved.versions().get(2).sequenceNumber());
    }

    @Test
    void testAtomicallyUpdateLogAndMetadata() throws Exception {
        long seq = 42L;
        int partition = 2;
        String key = "atomic.txt";
        
        strategy.atomicallyUpdateLogAndMetadata(
                new OpLog(partition, seq, key, Operation.DELETE),
                new Metadata(key, partition, List.of(new TombstoneFileVersion(seq))));

        assertInstanceOf(TombstoneFileVersion.class,
                strategy.getMetadata(key).orElseThrow().versions().get(0));

        try (Stream<OpLog> s = strategy.getLogs(partition, seq, seq)) {
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
    // Table #3 — Buckets
    // =========================================================================

    @Test
    void testCreateAndCheckBucketExists() throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(1, 5L, "animals", Operation.CREATE_BUCKET),
                new Bucket("animals", 1, 5L, false));

        var bucket = strategy.getBucket("animals").orElseThrow();
        assertFalse(bucket.deleted());
        assertEquals(5L, bucket.sequenceNumber());
    }

    @Test
    void testDeleteBucketTombstone() throws Exception {
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(1, 5L, "animals", Operation.CREATE_BUCKET),
                new Bucket("animals", 1, 5L, false));
        strategy.atomicallyUpdateLogAndBucket(
                new OpLog(1, 10L, "animals", Operation.DELETE_BUCKET),
                new Bucket("animals", 1, 10L, true));

        assertTrue(strategy.getBucket("animals").orElseThrow().deleted());
    }

    @Test
    void testBucketSerializationRoundTrip() {
        Bucket live = new Bucket("animals", 3, 42L, false);
        Bucket rtLive = Bucket.from(live.serialize());
        assertFalse(rtLive.deleted());
        assertEquals(42L, rtLive.sequenceNumber());

        Bucket dead = new Bucket("animals", 3, 99L, true);
        Bucket rtDead = Bucket.from(dead.serialize());
        assertTrue(rtDead.deleted());
    }

    @Test
    void testGetBucketReturnsEmptyForMissingKey() throws Exception {
        assertTrue(strategy.getBucket("nonexistent").isEmpty());
    }
}