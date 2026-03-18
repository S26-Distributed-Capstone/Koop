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
        database = new Database(new InMemoryStorageStrategy());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) {
            database.close();
        }
    }

    // =========================================================================
    // Table #1 — Operation Log
    // =========================================================================

    @Test
    void testLogOperationAndGetLogs() throws Exception {
        database.logOperation(new OpLog(1L, "file1.txt", Operation.PUT));
        database.logOperation(new OpLog(2L, "file2.txt", Operation.DELETE));

        try (Stream<OpLog> logStream = database.getLogs(2L, 1L)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());

            assertEquals(2, logs.size(), "Should retrieve exactly 2 logs");

            assertEquals(2L, logs.get(0).seqNum());
            assertEquals("file2.txt", logs.get(0).key());
            assertEquals(Operation.DELETE, logs.get(0).operation());

            assertEquals(1L, logs.get(1).seqNum());
            assertEquals("file1.txt", logs.get(1).key());
            assertEquals(Operation.PUT, logs.get(1).operation());
        }
    }

    @Test
    void testLogOpAndGetALog() throws Exception {
        database.logOperation(new OpLog(10, "singleFile.txt", Operation.PUT));
        var logOpt = database.getOpLog(10L);
        assertTrue(logOpt.isPresent());
        var log = logOpt.get();
        assertEquals(10L, log.seqNum());
        assertEquals("singleFile.txt", log.key());
        assertEquals(Operation.PUT, log.operation());
    }

    @Test
    void testGetOpLogReturnsEmptyForMissingKey() throws Exception {
        var logOpt = database.getOpLog(999L);
        assertTrue(logOpt.isEmpty(), "Should return empty Optional for missing log");
    }

    @Test
    void testLogOperationWithSameSequenceNumberOverrides() throws Exception {
        database.logOperation(new OpLog(5L, "file.txt", Operation.PUT));
        database.logOperation(new OpLog(5L, "file.txt", Operation.DELETE));
        var logOp = database.getOpLog(5L);
        assertEquals(Operation.DELETE, logOp.get().operation());
    }

    @Test
    void testGetLogsRange() throws Exception {
        for (long i = 1; i <= 5; i++) {
            database.logOperation(new OpLog(i, "file_" + i, Operation.PUT));
        }

        try (Stream<OpLog> logStream = database.getLogs(4L, 2L)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(3, logs.size());
            assertEquals(4L, logs.get(0).seqNum());
            assertEquals(3L, logs.get(1).seqNum());
            assertEquals(2L, logs.get(2).seqNum());
        }
    }

    @Test
    void testCreateBucketAndDeleteBucketOperationRoundTrip() {
        assertEquals(Operation.CREATE_BUCKET, Operation.fromString("CREATE_BUCKET"));
        assertEquals(Operation.CREATE_BUCKET, Operation.fromString("create_bucket"));
        assertEquals(Operation.DELETE_BUCKET, Operation.fromString("DELETE_BUCKET"));
        assertEquals(Operation.DELETE_BUCKET, Operation.fromString("delete_bucket"));

        // Verify they survive OpLog serialization round-trip
        OpLog log = new OpLog(7L, "animals", Operation.CREATE_BUCKET);
        OpLog roundTripped = OpLog.from(log.serialize());
        assertEquals(Operation.CREATE_BUCKET, roundTripped.operation());
        assertEquals("animals", roundTripped.key());
        assertEquals(7L, roundTripped.seqNum());
    }

    // =========================================================================
    // Table #2 — Metadata
    // =========================================================================

    @Test
    void testSetAndGetMetadata() throws Exception {
        String fileKey = "partition_1/fileA.dat";
        String location = "/data/p1/fileA.dat";
        int partition = 1;
        long seq = 100L;

        database.setMetadata(new Metadata(fileKey, location, partition, seq));

        var metadataOpt = database.getMetadata(fileKey);
        assertTrue(metadataOpt.isPresent());
        var metadata = metadataOpt.get();
        assertEquals(fileKey, metadata.fileName());
        assertEquals(location, metadata.location());
        assertEquals(partition, metadata.partition());
        assertEquals(seq, metadata.sequenceNumber());
    }

    @Test
    void testGetMetadataReturnsEmptyForMissingKey() throws Exception {
        var metadata = database.getMetadata("non_existent_file.txt");
        assertTrue(metadata.isEmpty());
    }

    @Test
    void testAtomicallyUpdate() throws Exception {
        long seq = 42L;
        String fileKey = "test-atomic.txt";
        var operation = Operation.PUT;
        String location = "/disk1/test-atomic.txt";
        int partition = 2;

        database.atomicallyUpdate(new Metadata(fileKey, location, partition, seq), new OpLog(seq, fileKey, operation));

        var metadataOpt = database.getMetadata(fileKey);
        assertTrue(metadataOpt.isPresent());
        var metadata = metadataOpt.get();
        assertEquals(location, metadata.location());
        assertEquals(partition, metadata.partition());
        assertEquals(seq, metadata.sequenceNumber());

        try (Stream<OpLog> logStream = database.getLogs(seq, seq)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(1, logs.size());
            assertEquals(operation, logs.get(0).operation());
            assertEquals(fileKey, logs.get(0).key());
        }
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        database.setMetadata(new Metadata("folderA/file1.txt", "loc1", 1, 1L));
        database.setMetadata(new Metadata("folderA/file2.txt", "loc2", 1, 2L));
        database.setMetadata(new Metadata("folderB/file3.txt", "loc3", 2, 3L));
        database.setMetadata(new Metadata("folderA_suffix/file4.txt", "loc4", 1, 4L));

        try (Stream<Metadata> metaStream = database.streamMetadataWithPrefix("folderA/")) {
            List<Metadata> results = metaStream
                    .filter(m -> m.fileName().startsWith("folderA/"))
                    .collect(Collectors.toList());

            assertEquals(2, results.size());
            List<String> fileNames = results.stream().map(Metadata::fileName).collect(Collectors.toList());
            assertTrue(fileNames.contains("folderA/file1.txt"));
            assertTrue(fileNames.contains("folderA/file2.txt"));
        }
    }

    @Test
    void testStreamMetadataWithPrefixReturnsEmptyForMissingPrefix() throws Exception {
        database.setMetadata(new Metadata("folderA/file1.txt", "loc1", 1, 1L));
        database.setMetadata(new Metadata("folderB/file2.txt", "loc2", 2, 2L));

        try (Stream<Metadata> metaStream = database.streamMetadataWithPrefix("nonexistent/")) {
            List<Metadata> results = metaStream
                    .filter(m -> m.fileName().startsWith("nonexistent/"))
                    .collect(Collectors.toList());
            assertTrue(results.isEmpty());
        }
    }

    // =========================================================================
    // Table #3 — Buckets
    // =========================================================================

    @Test
    void testPutAndGetBucket() throws Exception {
        Bucket bucket = new Bucket("animals", 0, 5L);
        database.putBucket(bucket);

        var result = database.getBucket("animals");
        assertTrue(result.isPresent());
        assertEquals("animals", result.get().key());
        assertEquals(0, result.get().partition());
        assertEquals(5L, result.get().sequenceNumber());
    }

    @Test
    void testGetBucketReturnsEmptyForMissingKey() throws Exception {
        var result = database.getBucket("nonexistent-bucket");
        assertTrue(result.isEmpty());
    }

    @Test
    void testDeleteBucket() throws Exception {
        database.putBucket(new Bucket("to-delete", 1, 10L));
        assertTrue(database.getBucket("to-delete").isPresent());

        database.deleteBucket("to-delete");
        assertTrue(database.getBucket("to-delete").isEmpty());
    }

    @Test
    void testDeleteBucketNoOpOnMissingKey() throws Exception {
        // Should not throw
        assertDoesNotThrow(() -> database.deleteBucket("never-existed"));
    }

    @Test
    void testStreamBuckets() throws Exception {
        database.putBucket(new Bucket("alpha", 0, 1L));
        database.putBucket(new Bucket("beta", 1, 2L));
        database.putBucket(new Bucket("gamma", 2, 3L));

        try (Stream<Bucket> stream = database.streamBuckets()) {
            List<Bucket> buckets = stream.collect(Collectors.toList());
            assertEquals(3, buckets.size());
            List<String> keys = buckets.stream().map(Bucket::key).collect(Collectors.toList());
            assertTrue(keys.contains("alpha"));
            assertTrue(keys.contains("beta"));
            assertTrue(keys.contains("gamma"));
        }
    }

    @Test
    void testBucketSerializationRoundTrip() {
        Bucket original = new Bucket("animals", 3, 42L);
        Bucket roundTripped = Bucket.from(original.serialize());
        assertEquals(original.key(), roundTripped.key());
        assertEquals(original.partition(), roundTripped.partition());
        assertEquals(original.sequenceNumber(), roundTripped.sequenceNumber());
    }

    // =========================================================================
    // Table #4 — Multipart Uploads
    // =========================================================================

    @Test
    void testPutAndGetMultipartUpload() throws Exception {
        MultipartUpload upload = new MultipartUpload("videos/big.mp4",
                List.of("data/chunk-0.blob", "data/chunk-1.blob", "data/chunk-2.blob"));
        database.putMultipartUpload(upload);

        var result = database.getMultipartUpload("videos/big.mp4");
        assertTrue(result.isPresent());
        assertEquals("videos/big.mp4", result.get().key());
        assertEquals(3, result.get().chunks().size());
        assertEquals("data/chunk-0.blob", result.get().chunks().get(0));
        assertEquals("data/chunk-1.blob", result.get().chunks().get(1));
        assertEquals("data/chunk-2.blob", result.get().chunks().get(2));
    }

    @Test
    void testGetMultipartUploadReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getMultipartUpload("no-such-key").isEmpty());
    }

    @Test
    void testDeleteMultipartUpload() throws Exception {
        database.putMultipartUpload(new MultipartUpload("foo/bar", List.of("chunk-0")));
        assertTrue(database.getMultipartUpload("foo/bar").isPresent());

        database.deleteMultipartUpload("foo/bar");
        assertTrue(database.getMultipartUpload("foo/bar").isEmpty());
    }

    @Test
    void testMultipartUploadWithEmptyChunkList() throws Exception {
        MultipartUpload upload = new MultipartUpload("empty/upload", List.of());
        database.putMultipartUpload(upload);

        var result = database.getMultipartUpload("empty/upload");
        assertTrue(result.isPresent());
        assertTrue(result.get().chunks().isEmpty());
    }

    @Test
    void testMultipartUploadSerializationRoundTrip() {
        MultipartUpload original = new MultipartUpload("doc/report.pdf",
                List.of("part-a", "part-b", "part-c"));
        MultipartUpload roundTripped = MultipartUpload.from(original.serialize());
        assertEquals(original.key(), roundTripped.key());
        assertEquals(original.chunks(), roundTripped.chunks());
    }

    // =========================================================================
    // Lifecycle — close clears all tables
    // =========================================================================

    @Test
    void testCloseClearsAllTables() throws Exception {
        database.setMetadata(new Metadata("file.txt", "loc", 1, 1L));
        database.logOperation(new OpLog(1L, "file.txt", Operation.PUT));
        database.putBucket(new Bucket("my-bucket", 0, 2L));
        database.putMultipartUpload(new MultipartUpload("file.txt", List.of("chunk-0")));

        database.close();

        assertTrue(database.getMetadata("file.txt").isEmpty());
        assertTrue(database.getOpLog(1L).isEmpty());
        assertTrue(database.getBucket("my-bucket").isEmpty());
        assertTrue(database.getMultipartUpload("file.txt").isEmpty());

        try (Stream<OpLog> logs = database.getLogs(1L, 1L)) {
            assertEquals(0, logs.count());
        }
    }
}