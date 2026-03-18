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
            assertEquals(2, logs.size());
            assertEquals(2L, logs.get(0).seqNum());
            assertEquals(Operation.DELETE, logs.get(0).operation());
            assertEquals(1L, logs.get(1).seqNum());
            assertEquals(Operation.PUT, logs.get(1).operation());
        }
    }

    @Test
    void testLogOpAndGetALog() throws Exception {
        database.logOperation(new OpLog(10, "singleFile.txt", Operation.PUT));
        var log = database.getOpLog(10L);
        assertTrue(log.isPresent());
        assertEquals(10L, log.get().seqNum());
        assertEquals(Operation.PUT, log.get().operation());
    }

    @Test
    void testGetOpLogReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getOpLog(999L).isEmpty());
    }

    @Test
    void testLogOperationWithSameSequenceNumberOverrides() throws Exception {
        database.logOperation(new OpLog(5L, "file.txt", Operation.PUT));
        database.logOperation(new OpLog(5L, "file.txt", Operation.DELETE));
        assertEquals(Operation.DELETE, database.getOpLog(5L).get().operation());
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
        assertEquals(Operation.DELETE_BUCKET, Operation.fromString("delete_bucket"));

        OpLog log = new OpLog(7L, "animals", Operation.CREATE_BUCKET);
        OpLog roundTripped = OpLog.from(log.serialize());
        assertEquals(Operation.CREATE_BUCKET, roundTripped.operation());
        assertEquals(7L, roundTripped.seqNum());
    }

    // =========================================================================
    // Table #2 — Metadata (with embedded FileVersion hierarchy)
    // =========================================================================

    @Test
    void testSetAndGetMetadataWithRegularVersion() throws Exception {
        String key = "partition_1/fileA.dat";
        List<FileVersion> versions = List.of(new RegularFileVersion(100L, "/disk1/fileA.dat"));

        database.setMetadata(new Metadata(key, 1, versions));

        var metadata = database.getMetadata(key).orElseThrow();
        assertEquals(key, metadata.key());
        assertEquals(1, metadata.partition());
        assertEquals(1, metadata.versions().size());
        assertInstanceOf(RegularFileVersion.class, metadata.versions().get(0));
        assertEquals(100L, metadata.versions().get(0).sequenceNumber());
        assertEquals("/disk1/fileA.dat", ((RegularFileVersion) metadata.versions().get(0)).location());
    }

    @Test
    void testGetMetadataReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getMetadata("non_existent_file.txt").isEmpty());
    }

    @Test
    void testMetadataSerializationRoundTripRegular() {
        Metadata original = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "/uuid1.blob"),
                        new RegularFileVersion(101L, "/uuid2.blob")));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(original.key(), rt.key());
        assertEquals(2, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertEquals(100L, rt.versions().get(0).sequenceNumber());
        assertEquals(101L, rt.versions().get(1).sequenceNumber());
    }

    @Test
    void testMetadataSerializationRoundTripMultipart() {
        Metadata original = new Metadata("animals/dog.jpg", 1,
                List.of(new MultipartFileVersion(98L, List.of("chunk-0.blob", "chunk-1.blob"))));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(1, rt.versions().size());
        assertInstanceOf(MultipartFileVersion.class, rt.versions().get(0));
        MultipartFileVersion mpv = (MultipartFileVersion) rt.versions().get(0);
        assertEquals(98L, mpv.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"), mpv.chunks());
    }

    @Test
    void testMetadataSerializationRoundTripMixed() {
        Metadata original = new Metadata("mixed/key", 2, List.of(
                new RegularFileVersion(10L, "/blob-10"),
                new MultipartFileVersion(20L, List.of("chunk-a", "chunk-b")),
                new RegularFileVersion(30L, "/blob-30")));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(3, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, rt.versions().get(1));
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(2));
        assertEquals(20L, rt.versions().get(1).sequenceNumber());
    }

    @Test
    void testAtomicallyUpdate() throws Exception {
        long seq = 42L;
        String key = "test-atomic.txt";
        List<FileVersion> versions = List.of(new RegularFileVersion(seq, "/disk1/test-atomic.txt"));

        database.atomicallyUpdate(new Metadata(key, 2, versions), new OpLog(seq, key, Operation.PUT));

        var metadata = database.getMetadata(key).orElseThrow();
        assertEquals(seq, metadata.versions().get(0).sequenceNumber());

        try (Stream<OpLog> logStream = database.getLogs(seq, seq)) {
            List<OpLog> logs = logStream.collect(Collectors.toList());
            assertEquals(1, logs.size());
            assertEquals(Operation.PUT, logs.get(0).operation());
        }
    }

    @Test
    void testStreamMetadataWithPrefix() throws Exception {
        database.setMetadata(new Metadata("folderA/file1.txt", 1, List.of(new RegularFileVersion(1L, "/l1"))));
        database.setMetadata(new Metadata("folderA/file2.txt", 1, List.of(new RegularFileVersion(2L, "/l2"))));
        database.setMetadata(new Metadata("folderB/file3.txt", 2, List.of(new RegularFileVersion(3L, "/l3"))));
        database.setMetadata(new Metadata("folderA_suffix/file4.txt", 1, List.of(new RegularFileVersion(4L, "/l4"))));

        try (Stream<Metadata> stream = database.streamMetadataWithPrefix("folderA/")) {
            List<String> keys = stream.filter(m -> m.key().startsWith("folderA/"))
                    .map(Metadata::key).collect(Collectors.toList());
            assertEquals(2, keys.size());
            assertTrue(keys.containsAll(List.of("folderA/file1.txt", "folderA/file2.txt")));
        }
    }

    // --- FileVersion helpers ---

    @Test
    void testPutAndGetFileVersions() throws Exception {
        String key = "animals/cat.jpg";
        List<FileVersion> versions = List.of(
                new RegularFileVersion(98L, "/disk1/cat-v98.dat"),
                new RegularFileVersion(100L, "/disk1/cat-v100.dat"));

        database.putFileVersions(key, 1, versions);

        var result = database.getFileVersions(key);
        assertTrue(result.isPresent());
        assertEquals(2, result.get().size());
        assertEquals(98L, result.get().get(0).sequenceNumber());
    }

    @Test
    void testGetFileVersionsReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getFileVersions("no-such-key").isEmpty());
    }

    // --- Multipart helpers ---

    @Test
    void testPutAndGetMultipartUpload() throws Exception {
        String key = "animals/dog.jpg";
        database.putMultipartUpload(key, 1, 98L, List.of("chunk-0.blob", "chunk-1.blob", "chunk-2.blob"));

        var result = database.getMultipartUpload(key);
        assertTrue(result.isPresent());
        assertEquals(3, result.get().size());
        assertEquals("chunk-0.blob", result.get().get(0));
    }

    @Test
    void testGetMultipartUploadReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getMultipartUpload("no-such-key").isEmpty());
    }

    @Test
    void testGetMultipartUploadReturnsEmptyWhenOnlyRegularVersions() throws Exception {
        database.setMetadata(new Metadata("key", 1, List.of(new RegularFileVersion(10L, "/blob"))));
        assertTrue(database.getMultipartUpload("key").isEmpty());
    }

    @Test
    void testDeleteMultipartUpload() throws Exception {
        String key = "animals/dog.jpg";
        database.setMetadata(new Metadata(key, 1, List.of(
                new RegularFileVersion(10L, "/blob-10"),
                new MultipartFileVersion(20L, List.of("chunk-0")))));

        database.deleteMultipartUpload(key);

        // MultipartFileVersion removed; RegularFileVersion still present
        var meta = database.getMetadata(key).orElseThrow();
        assertEquals(1, meta.versions().size());
        assertInstanceOf(RegularFileVersion.class, meta.versions().get(0));
        assertTrue(database.getMultipartUpload(key).isEmpty());
    }

    @Test
    void testMultipartPreservesRegularVersions() throws Exception {
        String key = "animals/dog.jpg";
        database.setMetadata(new Metadata(key, 1, List.of(new RegularFileVersion(98L, "/uuid3.blob"))));
        database.putMultipartUpload(key, 1, 99L, List.of("chunk-0.blob"));

        var meta = database.getMetadata(key).orElseThrow();
        assertEquals(2, meta.versions().size());
        assertInstanceOf(RegularFileVersion.class, meta.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, meta.versions().get(1));
    }

    // =========================================================================
    // Table #3 — Buckets
    // =========================================================================

    @Test
    void testPutAndGetBucket() throws Exception {
        database.putBucket(new Bucket("animals", 0, 5L));

        var result = database.getBucket("animals").orElseThrow();
        assertEquals("animals", result.key());
        assertEquals(0, result.partition());
        assertEquals(5L, result.sequenceNumber());
    }

    @Test
    void testGetBucketReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getBucket("nonexistent-bucket").isEmpty());
    }

    @Test
    void testDeleteBucket() throws Exception {
        database.putBucket(new Bucket("to-delete", 1, 10L));
        database.deleteBucket("to-delete");
        assertTrue(database.getBucket("to-delete").isEmpty());
    }

    @Test
    void testDeleteBucketNoOpOnMissingKey() throws Exception {
        assertDoesNotThrow(() -> database.deleteBucket("never-existed"));
    }

    @Test
    void testStreamBuckets() throws Exception {
        database.putBucket(new Bucket("alpha", 0, 1L));
        database.putBucket(new Bucket("beta", 1, 2L));
        database.putBucket(new Bucket("gamma", 2, 3L));

        try (Stream<Bucket> stream = database.streamBuckets()) {
            List<String> keys = stream.map(Bucket::key).collect(Collectors.toList());
            assertTrue(keys.containsAll(List.of("alpha", "beta", "gamma")));
        }
    }

    @Test
    void testBucketSerializationRoundTrip() {
        Bucket original = new Bucket("animals", 3, 42L);
        Bucket rt = Bucket.from(original.serialize());
        assertEquals(original.key(), rt.key());
        assertEquals(original.partition(), rt.partition());
        assertEquals(original.sequenceNumber(), rt.sequenceNumber());
    }

    // =========================================================================
    // Lifecycle — close clears all tables
    // =========================================================================

    @Test
    void testCloseClearsAllTables() throws Exception {
        database.setMetadata(new Metadata("file.txt", 1,
                List.of(new RegularFileVersion(1L, "/disk1/file.txt"))));
        database.logOperation(new OpLog(1L, "file.txt", Operation.PUT));
        database.putBucket(new Bucket("my-bucket", 0, 2L));

        database.close();

        assertTrue(database.getMetadata("file.txt").isEmpty());
        assertTrue(database.getOpLog(1L).isEmpty());
        assertTrue(database.getBucket("my-bucket").isEmpty());
        try (Stream<OpLog> logs = database.getLogs(1L, 1L)) {
            assertEquals(0, logs.count());
        }
    }
}