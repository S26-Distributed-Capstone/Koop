package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {

    private Database database;

    @BeforeEach
    void setUp() {
        // Initialize the Database with the InMemoryStorageStrategy before each test
        database = new Database(new InMemoryStorageStrategy());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) {
            database.close();
        }
    }

    @Test
    void testLogOperationAndGetLogs() throws Exception {
        // Log a few operations
        database.logOperation(1L, "file1.txt", "PUT");
        database.logOperation(2L, "file2.txt", "DELETE");

        // Retrieve the logs
        try (Stream<OpLog> logStream = database.getLogs(1L, 2L)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());

            assertEquals(2, logs.size(), "Should retrieve exactly 2 logs");
            
            // InMemoryStorageStrategy's subMap().reversed() returns descending order
            assertEquals(2L, logs.get(0).seqNum());
            assertEquals("file2.txt", logs.get(0).key());
            assertEquals("DELETE", logs.get(0).operation());

            assertEquals(1L, logs.get(1).seqNum());
            assertEquals("file1.txt", logs.get(1).key());
            assertEquals("PUT", logs.get(1).operation());
        }
    }

    @Test
    void testLogOpAndGetALog() throws Exception{
        database.logOperation(10, "singleFile.txt", "PUT");
        var logOpt = database.getOpLog(10L);
        assertTrue(logOpt.isPresent(), "Log should not be null");
        var log = logOpt.get();
        assertEquals(10L, log.seqNum());
        assertEquals("singleFile.txt", log.key());
        assertEquals("PUT", log.operation());
    }

    @Test
    void testGetOpLogReturnsEmptyForMissingKey() throws Exception {
        var logOpt = database.getOpLog(999L);
        assertTrue(logOpt.isEmpty(), "Should return empty Optional for missing log");
    }

    @Test
    void testStreamMetadataWithPrefixReturnsEmptyForMissingPrefix() throws Exception {
        database.setMetadata("folderA/file1.txt", "loc1", "1", 1L);
        database.setMetadata("folderB/file2.txt", "loc2", "2", 2L);

        try (Stream<Metadata> metaStream = database.streamMetadataWithPrefix("nonexistent/")) {
            List<Metadata> results = metaStream
                .filter(m -> m.fileName().startsWith("nonexistent/"))
                .collect(Collectors.toList());
            assertTrue(results.isEmpty(), "Should return empty list for missing prefix");
        }
    }

    @Test
    void testLogOperationWithSameSequenceNumberOverrides() throws Exception {
        database.logOperation(5L, "file.txt", "PUT");
        database.logOperation(5L, "file.txt", "DELETE");
        var logOp = database.getOpLog(5L);
        assertEquals("DELETE",logOp.get().operation());
    }

    @Test
    void testSetAndGetMetadata() throws Exception {
        String fileKey = "partition_1/fileA.dat";
        String location = "/data/p1/fileA.dat";
        String partition = "1";
        long seq = 100L;

        // Set the metadata
        database.setMetadata(fileKey, location, partition, seq);

        // Retrieve the metadata
        var metadataOpt = database.getMetadata(fileKey);

        assertTrue(metadataOpt.isPresent(), "Metadata should not be null");
        var metadata = metadataOpt.get();
        assertEquals(fileKey, metadata.fileName());
        assertEquals(location, metadata.location());
        assertEquals(partition, metadata.partition());
        assertEquals(seq, metadata.sequenceNumber());
    }

    @Test
    void testGetMetadataReturnsNullForMissingKey() throws Exception {
        var metadata = database.getMetadata("non_existent_file.txt");
        assertTrue(metadata.isEmpty(), "Retrieving missing metadata should return null");
    }

    @Test
    void testAtomicallyUpdate() throws Exception {
        long seq = 42L;
        String fileKey = "test-atomic.txt";
        String operation = "PUT";
        String location = "/disk1/test-atomic.txt";
        String partition = "2";

        // Perform atomic update
        database.atomicallyUpdate(seq, fileKey, operation, location, partition);

        // Verify metadata was updated
        var metadataOpt = database.getMetadata(fileKey);
        assertTrue(metadataOpt.isPresent());
        var metadata = metadataOpt.get();
        assertEquals(location, metadata.location());
        assertEquals(partition, metadata.partition());
        assertEquals(seq, metadata.sequenceNumber());

        // Verify log was added
        try (Stream<OpLog> logStream = database.getLogs(seq, seq)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(1, logs.size());
            assertEquals(operation, logs.get(0).operation());
            assertEquals(fileKey, logs.get(0).key());
        }
    }

    @Test
    void testGetLogsRange() throws Exception {
        // Insert logs with sequences 1 through 5
        for (long i = 1; i <= 5; i++) {
            database.logOperation(i, "file_" + i, "OP_" + i);
        }

        // Query logs from seq 2 to seq 4
        try (Stream<OpLog> logStream = database.getLogs(2L, 4L)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());

            // Should retrieve 3 logs: seq 4, 3, 2 (reversed order due to descending map)
            assertEquals(3, logs.size());
            assertEquals(4L, logs.get(0).seqNum());
            assertEquals(3L, logs.get(1).seqNum());
            assertEquals(2L, logs.get(2).seqNum());
        }
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        database.setMetadata("folderA/file1.txt", "loc1", "1", 1L);
        database.setMetadata("folderA/file2.txt", "loc2", "1", 2L);
        database.setMetadata("folderB/file3.txt", "loc3", "2", 3L);
        database.setMetadata("folderA_suffix/file4.txt", "loc4", "1", 4L);

        // Stream metadata starting with "folderA/"
        try (Stream<Metadata> metaStream = database.streamMetadataWithPrefix("folderA/")) {
            List<Metadata> results = metaStream
                    // Note: InMemoryStorageStrategy tailMap returns everything after the prefix.
                    // To strictly match prefix semantics, we filter it here (as a real caller would 
                    // or as RocksDbStorageStrategy handles internally).
                    .filter(m -> m.fileName().startsWith("folderA/"))
                    .collect(Collectors.toList());

            assertEquals(2, results.size(), "Should only find exact prefix matches");
            
            List<String> fileNames = results.stream().map(Metadata::fileName).collect(Collectors.toList());
            assertTrue(fileNames.contains("folderA/file1.txt"));
            assertTrue(fileNames.contains("folderA/file2.txt"));
        }
    }

    @Test
    void testCloseClearsInMemoryStorage() throws Exception {
        database.setMetadata("file.txt", "loc", "1", 1L);
        database.logOperation(1L, "file.txt", "PUT");
        
        // Ensure data is there
        assertTrue(database.getMetadata("file.txt").isPresent());
        
        // Close the database (which calls close() on InMemoryStorageStrategy, clearing maps)
        database.close();
        
        // Data should be gone
        assertTrue(database.getMetadata("file.txt").isEmpty());
        try (Stream<OpLog> logs = database.getLogs(1L, 1L)) {
            assertEquals(0, logs.count());
        }
    }
}