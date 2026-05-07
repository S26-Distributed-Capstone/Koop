package com.github.koop.storagenode.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {

    @TempDir
    Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() throws RocksDBException {
        database = new Database(new RocksDbStorageStrategy(tempDir.toAbsolutePath().toString()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) database.close();
    }

    @Test
    void testPutItemCreatesMetadataAndLog() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "uuid-100.blob", 1024L);

        var meta = database.getItem("animals/cat.jpg").orElseThrow();
        assertEquals(1, meta.versions().size());
        assertInstanceOf(RegularFileVersion.class, meta.versions().get(0));
        assertEquals(100L, meta.versions().get(0).sequenceNumber());
        assertEquals("uuid-100.blob", ((RegularFileVersion) meta.versions().get(0)).location());
        assertEquals(1024L, meta.versions().get(0).size());
        assertFalse(((RegularFileVersion) meta.versions().get(0)).materialized());
    }

    @Test
    void testPutItemReturnsFalseWhenBlobNotPreRegistered() throws Exception {
        boolean materialized = database.putItem("animals/cat.jpg", 1, 100L, "uuid-100.blob", 1024L);
        assertFalse(materialized, "putItem should return false when no prior registerBlobArrival");
    }

    @Test
    void testPutItemReturnsTrueWhenBlobPreRegistered() throws Exception {
        database.registerBlobArrival("animals/cat.jpg", "uuid-100.blob", System.currentTimeMillis());

        boolean materialized = database.putItem("animals/cat.jpg", 1, 100L, "uuid-100.blob", 1024L);
        assertTrue(materialized, "putItem should return true when blob was pre-registered");

        var version = (RegularFileVersion) database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertTrue(version.materialized(), "RegularFileVersion.materialized should be true");
    }

    @Test
    void testPutItemCleansUpUncommittedEntryOnMaterialization() throws Exception {
        String reqId = "uuid-cleanup.blob";
        database.putUncommittedWrite(reqId, System.currentTimeMillis());

        boolean materialized = database.putItem("animals/cat.jpg", 1, 100L, reqId, 1024L);
        assertTrue(materialized);

        boolean second = database.putItem("animals/cat.jpg2", 1, 101L, reqId, 1024L);
        assertFalse(second, "Uncommitted entry should have been removed after first materialization");
    }

    @Test
    void testPutItemAppendsVersions() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "uuid-100.blob", 1024L);
        database.putItem("animals/cat.jpg", 1, 101L, "uuid-101.blob", 2048L);

        var versions = database.getItem("animals/cat.jpg").orElseThrow().versions();
        assertEquals(2, versions.size());
        assertEquals(100L, versions.get(0).sequenceNumber());
        assertEquals(101L, versions.get(1).sequenceNumber());
    }

    @Test
    void testPutMultipartItemCreatesMultipartFileVersion() throws Exception {
        database.putMultipartItem("animals/dog.jpg", 1, 98L,
                List.of("chunk-0.blob", "chunk-1.blob", "chunk-2.blob"), 5000L);

        var meta = database.getItem("animals/dog.jpg").orElseThrow();
        assertEquals(1, meta.versions().size());
        assertInstanceOf(MultipartFileVersion.class, meta.versions().get(0));
        var mpv = (MultipartFileVersion) meta.versions().get(0);
        assertEquals(98L, mpv.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob", "chunk-2.blob"), mpv.chunks());
        assertEquals(5000L, mpv.size());
    }

    @Test
    void testPutMultipartItemPreservesExistingVersions() throws Exception {
        database.putItem("animals/dog.jpg", 1, 10L, "/uuid-10.blob", 1024L);
        database.putMultipartItem("animals/dog.jpg", 1, 20L, List.of("chunk-0.blob"), 5000L);

        var versions = database.getItem("animals/dog.jpg").orElseThrow().versions();
        assertEquals(2, versions.size());
        assertInstanceOf(RegularFileVersion.class, versions.get(0));
        assertInstanceOf(MultipartFileVersion.class, versions.get(1));
    }

    @Test
    void testDeleteItemAppendsTombstone() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob", 1024L);
        database.deleteItem("animals/cat.jpg", 1, 101L);

        var versions = database.getItem("animals/cat.jpg").orElseThrow().versions();
        assertEquals(2, versions.size());
        assertInstanceOf(RegularFileVersion.class, versions.get(0));
        assertInstanceOf(TombstoneFileVersion.class, versions.get(1));
        assertEquals(101L, versions.get(1).sequenceNumber());
    }

    @Test
    void testDeleteItemOnNonExistentKeyCreatesTombstoneRow() throws Exception {
        database.deleteItem("ghost/file.txt", 1, 50L);

        var versions = database.getItem("ghost/file.txt").orElseThrow().versions();
        assertEquals(1, versions.size());
        assertInstanceOf(TombstoneFileVersion.class, versions.get(0));
        assertEquals(50L, versions.get(0).sequenceNumber());
    }

    @Test
    void testGetItemReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getItem("no-such-key").isEmpty());
    }

    @Test
    void testGetLatestFileVersionReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getLatestFileVersion("no-such-key").isEmpty());
    }

    @Test
    void testGetLatestFileVersionReturnsSingleVersion() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob", 1024L);

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertInstanceOf(RegularFileVersion.class, latest);
        assertEquals(100L, latest.sequenceNumber());
        assertEquals("/uuid-100.blob", ((RegularFileVersion) latest).location());
    }

    @Test
    void testGetLatestFileVersionReturnsLastVersionWhenMultipleExist() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob", 1024L);
        database.putItem("animals/cat.jpg", 1, 101L, "/uuid-101.blob", 1024L);
        database.putItem("animals/cat.jpg", 1, 102L, "/uuid-102.blob", 1024L);

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertEquals(102L, latest.sequenceNumber());
        assertEquals("/uuid-102.blob", ((RegularFileVersion) latest).location());
    }

    @Test
    void testGetLatestFileVersionReturnsMultipartVersionWhenLatest() throws Exception {
        database.putItem("animals/dog.jpg", 1, 10L, "/uuid-10.blob", 1024L);
        database.putMultipartItem("animals/dog.jpg", 1, 20L, List.of("chunk-0.blob", "chunk-1.blob"), 5000L);

        var latest = database.getLatestFileVersion("animals/dog.jpg").orElseThrow();
        assertInstanceOf(MultipartFileVersion.class, latest);
        assertEquals(20L, latest.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"), ((MultipartFileVersion) latest).chunks());
    }

    @Test
    void testGetLatestFileVersionReturnsTombstoneWhenDeleted() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob", 1024L);
        database.deleteItem("animals/cat.jpg", 1, 101L);

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertInstanceOf(TombstoneFileVersion.class, latest);
        assertEquals(101L, latest.sequenceNumber());
    }

    @Test
    void testCreateBucketAndBucketExists() throws Exception {
        assertFalse(database.bucketExists("animals"));
        database.createBucket("animals", 1, 5L);
        assertTrue(database.bucketExists("animals"));
    }

    @Test
    void testDeleteBucketTombstones() throws Exception {
        database.createBucket("animals", 1, 5L);
        assertTrue(database.bucketExists("animals"));
        database.deleteBucket("animals", 1, 10L);
        assertFalse(database.bucketExists("animals"));
    }

    @Test
    void testDeleteBucketOnNonExistentKeyIsHandled() throws Exception {
        assertDoesNotThrow(() -> database.deleteBucket("ghost-bucket", 1, 1L));
        assertFalse(database.bucketExists("ghost-bucket"));
    }

    @Test
    void testBucketExistsReturnsFalseForMissingKey() throws Exception {
        assertFalse(database.bucketExists("nonexistent"));
    }

    @Test
    void testListItemsInBucket() throws Exception {
        database.putItem("photos/cat.jpg", 1, 1L, "/blob-1", 1024L);
        database.putItem("photos/dog.jpg", 1, 2L, "/blob-2", 1024L);
        database.putItem("videos/clip.mp4", 1, 3L, "/blob-3", 1024L);
        database.putItem("photos_backup/cat.jpg", 1, 4L, "/blob-4", 1024L);

        try (Stream<Metadata> stream = database.listItemsInBucket("photos/")) {
            List<String> keys = stream.map(Metadata::key).collect(Collectors.toList());
            assertEquals(2, keys.size());
            assertTrue(keys.containsAll(List.of("photos/cat.jpg", "photos/dog.jpg")));
        }
    }

    @Test
    void testListItemsInBucketReturnsEmptyForUnknownPrefix() throws Exception {
        database.putItem("photos/cat.jpg", 1, 1L, "/blob-1", 1024L);
        try (Stream<Metadata> stream = database.listItemsInBucket("videos/")) {
            assertTrue(stream.collect(Collectors.toList()).isEmpty());
        }
    }

    @Test
    void testMetadataSerializationRoundTripRegular() {
        Metadata original = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "uuid1.blob", true, 500L),
                        new RegularFileVersion(101L, "uuid2.blob", false, 0L)));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(2, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertEquals(100L, rt.versions().get(0).sequenceNumber());
        assertEquals(500L, rt.versions().get(0).size());
        assertTrue(((RegularFileVersion) rt.versions().get(0)).materialized());
        assertFalse(((RegularFileVersion) rt.versions().get(1)).materialized());
    }

    @Test
    void testMetadataSerializationRoundTripMultipart() {
        Metadata original = new Metadata("animals/dog.jpg", 1,
                List.of(new MultipartFileVersion(98L, List.of("chunk-0.blob", "chunk-1.blob"), 5000L)));
        Metadata rt = Metadata.from(original.serialize());
        assertInstanceOf(MultipartFileVersion.class, rt.versions().get(0));
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"),
                ((MultipartFileVersion) rt.versions().get(0)).chunks());
        assertEquals(5000L, rt.versions().get(0).size());
    }

    @Test
    void testMetadataSerializationRoundTripTombstone() {
        Metadata original = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "/uuid1.blob", true, 1024L),
                        new TombstoneFileVersion(101L)));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(2, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertInstanceOf(TombstoneFileVersion.class, rt.versions().get(1));
        assertEquals(101L, rt.versions().get(1).sequenceNumber());
    }

    @Test
    void testMetadataSerializationRoundTripAllThreeTypes() {
        Metadata original = new Metadata("mixed/key", 2, List.of(
                new RegularFileVersion(10L, "/blob-10", true, 1024L),
                new MultipartFileVersion(20L, List.of("chunk-a", "chunk-b"), 5000L),
                new TombstoneFileVersion(30L)));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(3, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertInstanceOf(MultipartFileVersion.class, rt.versions().get(1));
        assertInstanceOf(TombstoneFileVersion.class, rt.versions().get(2));
        assertEquals(30L, rt.versions().get(2).sequenceNumber());
    }
}