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
        if (strategy != null) {
            strategy.close();
        }
    }

    // =========================================================================
    // Table #2 — Metadata with FileVersion hierarchy
    // =========================================================================

    @Test
    void testUpdateAndGetMetadataWithRegularVersion() throws Exception {
        String key = "partition_1/fileA.dat";
        Metadata meta = new Metadata(key, 1,
                List.of(new RegularFileVersion(100L, "/data/p1/fileA.dat")));

        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata(key).orElseThrow();
        assertEquals(key, retrieved.key());
        assertEquals(1, retrieved.partition());
        assertEquals(1, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertEquals(100L, retrieved.versions().get(0).sequenceNumber());
        assertEquals("/data/p1/fileA.dat", ((RegularFileVersion) retrieved.versions().get(0)).location());
    }

    @Test
    void testUpdateAndGetMetadataWithMultipartVersion() throws Exception {
        String key = "animals/dog.jpg";
        Metadata meta = new Metadata(key, 1,
                List.of(new MultipartFileVersion(98L, List.of("chunk-0.blob", "chunk-1.blob"))));

        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata(key).orElseThrow();
        assertEquals(1, retrieved.versions().size());
        assertInstanceOf(MultipartFileVersion.class, retrieved.versions().get(0));
        MultipartFileVersion mpv = (MultipartFileVersion) retrieved.versions().get(0);
        assertEquals(98L, mpv.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"), mpv.chunks());
    }

    @Test
    void testUpdateAndGetMetadataMixedVersions() throws Exception {
        String key = "mixed/key";
        Metadata meta = new Metadata(key, 2, List.of(
                new RegularFileVersion(10L, "/blob-10"),
                new MultipartFileVersion(20L, List.of("chunk-a", "chunk-b")),
                new RegularFileVersion(30L, "/blob-30")));

        strategy.updateMetadata(meta);

        var retrieved = strategy.getMetadata(key).orElseThrow();
        assertEquals(3, retrieved.versions().size());
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, retrieved.versions().get(1));
        assertInstanceOf(RegularFileVersion.class, retrieved.versions().get(2));
        assertEquals(20L, retrieved.versions().get(1).sequenceNumber());
    }

    @Test
    void testGetMetadataReturnsEmptyForMissingKey() throws Exception {
        assertTrue(strategy.getMetadata("missing_file.dat").isEmpty());
    }

    @Test
    void testAtomicallyUpdateLogAndMetadata() throws Exception {
        long seq = 42L;
        String key = "atomic_file.txt";

        OpLog log = new OpLog(seq, key, Operation.DELETE);
        Metadata meta = new Metadata(key, 2,
                List.of(new RegularFileVersion(seq, Database.TOMBSTONE_LOCATION)));

        strategy.atomicallyUpdateLogAndMetadata(log, meta);

        var retrievedMeta = strategy.getMetadata(key).orElseThrow();
        assertEquals(Database.TOMBSTONE_LOCATION,
                ((RegularFileVersion) retrievedMeta.versions().get(0)).location());

        try (Stream<OpLog> logStream = strategy.getLogs(seq, seq)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(1, logs.size());
            assertEquals(Operation.DELETE, logs.get(0).operation());
        }
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        strategy.updateMetadata(new Metadata("videos/2023/vid1.mp4", 1, List.of(new RegularFileVersion(1L, "/l1"))));
        strategy.updateMetadata(new Metadata("videos/2023/vid2.mp4", 1, List.of(new RegularFileVersion(2L, "/l2"))));
        strategy.updateMetadata(new Metadata("videos/2024/vid3.mp4", 1, List.of(new RegularFileVersion(3L, "/l3"))));
        strategy.updateMetadata(new Metadata("documents/doc1.pdf",   1, List.of(new RegularFileVersion(4L, "/l4"))));
        strategy.updateMetadata(new Metadata("videos/202",           1, List.of(new RegularFileVersion(5L, "/l5"))));

        try (Stream<Metadata> stream = strategy.streamMetadataWithPrefix("videos/2023/")) {
            List<String> keys = stream.map(Metadata::key).collect(Collectors.toList());
            assertEquals(2, keys.size());
            assertTrue(keys.containsAll(List.of("videos/2023/vid1.mp4", "videos/2023/vid2.mp4")));
        }

        try (Stream<Metadata> stream = strategy.streamMetadataWithPrefix("videos/")) {
            assertEquals(4, stream.collect(Collectors.toList()).size());
        }
    }

    @Test
    void testPrefixStreamReturnsEmptyForNoMatch() throws Exception {
        strategy.updateMetadata(new Metadata("folderA/file1.txt", 1, List.of()));
        try (Stream<Metadata> stream = strategy.streamMetadataWithPrefix("folderB/")) {
            assertTrue(stream.collect(Collectors.toList()).isEmpty());
        }
    }

    // =========================================================================
    // Table #1 — Operation Log
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
    // Table #3 — Buckets
    // =========================================================================

    @Test
    void testPutAndGetBucket() throws Exception {
        strategy.putBucket(new Bucket("animals", 0, 5L));
        var result = strategy.getBucket("animals").orElseThrow();
        assertEquals("animals", result.key());
        assertEquals(5L, result.sequenceNumber());
    }

    @Test
    void testDeleteBucket() throws Exception {
        strategy.putBucket(new Bucket("to-delete", 1, 10L));
        strategy.deleteBucket("to-delete");
        assertTrue(strategy.getBucket("to-delete").isEmpty());
    }
}