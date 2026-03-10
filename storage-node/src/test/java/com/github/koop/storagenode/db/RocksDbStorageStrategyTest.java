package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.etcd.jetcd.op.Cmp.Op;

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
        // Initialize the strategy with a fresh temporary directory for RocksDB
        strategy = new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (strategy != null) {
            strategy.close();
        }
    }

    @Test
    void testUpdateAndGetMetadata() throws Exception {
        String fileKey = "partition_1/fileA.dat";
        Metadata meta = new Metadata(fileKey, "/data/p1/fileA.dat", "1", 100L);

        // Save metadata
        strategy.updateMetadata(meta);

        // Retrieve and verify
        var retrieved = strategy.getMetadata(fileKey);
        assertTrue(retrieved.isPresent(), "Metadata should be found");
        var metadata = retrieved.get();
        assertEquals(fileKey, metadata.fileName());
        assertEquals("/data/p1/fileA.dat", metadata.location());
        assertEquals("1", metadata.partition());
        assertEquals(100L, metadata.sequenceNumber());
    }

    @Test
    void testGetMetadataReturnsEmptyForMissingKey() throws Exception {
        var retrieved = strategy.getMetadata("missing_file.dat");
        assertTrue(retrieved.isEmpty(), "Missing keys should return an empty Optional");
    }

    @Test
    void testAddLogAndGetLogsRange() throws Exception {
        // Add 5 logs
        for (long i = 1; i <= 5; i++) {
            strategy.addLog(new OpLog(i, "file_" + i, Operation.PUT));
        }

        // Retrieve logs between sequence numbers 2 and 4
        // According to the interface contract and our implementation, it iterates backwards
        try (Stream<OpLog> logStream = strategy.getLogs(2L, 4L)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());

            assertEquals(3, logs.size(), "Should retrieve exactly 3 logs");
            
            // Should be in descending order: 4, 3, 2
            assertEquals(4L, logs.get(0).seqNum());
            assertEquals(3L, logs.get(1).seqNum());
            assertEquals(2L, logs.get(2).seqNum());
            
            assertEquals("file_4", logs.get(0).key());
        }
    }

    @Test
    void testAtomicallyUpdateLogAndMetadata() throws Exception {
        long seq = 42L;
        String fileKey = "atomic_file.txt";
        
        OpLog log = new OpLog(seq, fileKey, Operation.DELETE);
        Metadata meta = new Metadata(fileKey, "deleted", "2", seq);

        // Perform atomic update
        strategy.atomicallyUpdateLogAndMetadata(log, meta);

        // Verify Metadata exists
        var retrievedMetaOpt = strategy.getMetadata(fileKey);
        assertTrue(retrievedMetaOpt.isPresent());
        assertEquals(seq, retrievedMetaOpt.get().sequenceNumber());

        // Verify Log exists
        try (Stream<OpLog> logStream = strategy.getLogs(seq, seq)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(1, logs.size());
            assertEquals(seq, logs.get(0).seqNum());
            assertEquals(Operation.DELETE, logs.get(0).operation());
        }
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        // Setup data with different prefixes
        strategy.updateMetadata(new Metadata("videos/2023/vid1.mp4", "loc1", "1", 1L));
        strategy.updateMetadata(new Metadata("videos/2023/vid2.mp4", "loc2", "1", 2L));
        strategy.updateMetadata(new Metadata("videos/2024/vid3.mp4", "loc3", "1", 3L));
        strategy.updateMetadata(new Metadata("documents/doc1.pdf", "loc4", "1", 4L));
        strategy.updateMetadata(new Metadata("videos/202", "loc5", "1", 5L)); // Partial match trap

        // Query prefix "videos/2023/"
        try (Stream<Metadata> metaStream = strategy.streamMetadataWithPrefix("videos/2023/")) {
            List<Metadata> results = metaStream.collect(Collectors.toList());

            assertEquals(2, results.size(), "Should find exactly 2 files matching the prefix");
            
            List<String> fileNames = results.stream().map(Metadata::fileName).collect(Collectors.toList());
            assertTrue(fileNames.contains("videos/2023/vid1.mp4"));
            assertTrue(fileNames.contains("videos/2023/vid2.mp4"));
        }

        // Query prefix "videos/"
        try (Stream<Metadata> metaStream = strategy.streamMetadataWithPrefix("videos/")) {
            List<Metadata> results = metaStream.collect(Collectors.toList());
            assertEquals(4, results.size(), "Should find all 4 video files");
        }
    }

    @Test
    void testPrefixStreamReturnsEmptyForNoMatch() throws Exception {
        strategy.updateMetadata(new Metadata("folderA/file1.txt", "loc1", "1", 1L));

        try (Stream<Metadata> metaStream = strategy.streamMetadataWithPrefix("folderB/")) {
            List<Metadata> results = metaStream.collect(Collectors.toList());
            assertTrue(results.isEmpty(), "Prefix with no matches should return empty stream");
        }
    }
}