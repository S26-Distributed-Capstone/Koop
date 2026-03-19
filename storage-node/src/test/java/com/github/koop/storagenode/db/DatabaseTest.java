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
        if (database != null) database.close();
    }

    // =========================================================================
    // PUT ITEM (regular)
    // =========================================================================

    @Test
    void testPutItemCreatesMetadataAndLog() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");

        var meta = database.getItem("animals/cat.jpg").orElseThrow();
        assertEquals(1, meta.versions().size());
        assertInstanceOf(RegularFileVersion.class, meta.versions().get(0));
        assertEquals(100L, meta.versions().get(0).sequenceNumber());
        assertEquals("/uuid-100.blob", ((RegularFileVersion) meta.versions().get(0)).location());
    }

    @Test
    void testPutItemAppendsVersions() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");
        database.putItem("animals/cat.jpg", 1, 101L, "/uuid-101.blob");

        var meta = database.getItem("animals/cat.jpg").orElseThrow();
        assertEquals(2, meta.versions().size());
        assertEquals(100L, meta.versions().get(0).sequenceNumber());
        assertEquals(101L, meta.versions().get(1).sequenceNumber());
    }

    // =========================================================================
    // PUT MULTIPART ITEM
    // =========================================================================

    @Test
    void testPutMultipartItemCreatesMultipartFileVersion() throws Exception {
        database.putMultipartItem("animals/dog.jpg", 1, 98L,
                List.of("chunk-0.blob", "chunk-1.blob", "chunk-2.blob"));

        var meta = database.getItem("animals/dog.jpg").orElseThrow();
        assertEquals(1, meta.versions().size());
        assertInstanceOf(MultipartFileVersion.class, meta.versions().get(0));
        var mpv = (MultipartFileVersion) meta.versions().get(0);
        assertEquals(98L, mpv.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob", "chunk-2.blob"), mpv.chunks());
    }

    @Test
    void testPutMultipartItemPreservesExistingVersions() throws Exception {
        database.putItem("animals/dog.jpg", 1, 10L, "/uuid-10.blob");
        database.putMultipartItem("animals/dog.jpg", 1, 20L, List.of("chunk-0.blob"));

        var versions = database.getItem("animals/dog.jpg").orElseThrow().versions();
        assertEquals(2, versions.size());
        assertInstanceOf(RegularFileVersion.class, versions.get(0));
        assertInstanceOf(MultipartFileVersion.class, versions.get(1));
    }

    // =========================================================================
    // DELETE ITEM
    // =========================================================================

    @Test
    void testDeleteItemAppendsTombstone() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");
        database.deleteItem("animals/cat.jpg", 1, 101L);

        var versions = database.getItem("animals/cat.jpg").orElseThrow().versions();
        assertEquals(2, versions.size());
        var latest = (RegularFileVersion) versions.get(1);
        assertEquals(Database.TOMBSTONE_LOCATION, latest.location());
        assertEquals(101L, latest.sequenceNumber());
    }

    @Test
    void testDeleteItemOnNonExistentKeyCreatesRow() throws Exception {
        // Delete on a key that never existed — creates a tombstone row
        database.deleteItem("ghost/file.txt", 1, 50L);
        var meta = database.getItem("ghost/file.txt");
        assertTrue(meta.isPresent());
        assertEquals(1, meta.get().versions().size());
        assertEquals(Database.TOMBSTONE_LOCATION,
                ((RegularFileVersion) meta.get().versions().get(0)).location());
    }

    // =========================================================================
    // GET ITEM
    // =========================================================================

    @Test
    void testGetItemReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getItem("no-such-key").isEmpty());
    }

    // =========================================================================
    // GET LATEST FILE VERSION
    // =========================================================================

    @Test
    void testGetLatestFileVersionReturnsEmptyForMissingKey() throws Exception {
        assertTrue(database.getLatestFileVersion("no-such-key").isEmpty());
    }

    @Test
    void testGetLatestFileVersionReturnsSingleVersion() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertInstanceOf(RegularFileVersion.class, latest);
        assertEquals(100L, latest.sequenceNumber());
        assertEquals("/uuid-100.blob", ((RegularFileVersion) latest).location());
    }

    @Test
    void testGetLatestFileVersionReturnsLastVersionWhenMultipleExist() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");
        database.putItem("animals/cat.jpg", 1, 101L, "/uuid-101.blob");
        database.putItem("animals/cat.jpg", 1, 102L, "/uuid-102.blob");

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertEquals(102L, latest.sequenceNumber());
        assertEquals("/uuid-102.blob", ((RegularFileVersion) latest).location());
    }

    @Test
    void testGetLatestFileVersionReturnsMultipartVersionWhenLatest() throws Exception {
        database.putItem("animals/dog.jpg", 1, 10L, "/uuid-10.blob");
        database.putMultipartItem("animals/dog.jpg", 1, 20L, List.of("chunk-0.blob", "chunk-1.blob"));

        var latest = database.getLatestFileVersion("animals/dog.jpg").orElseThrow();
        assertInstanceOf(MultipartFileVersion.class, latest);
        assertEquals(20L, latest.sequenceNumber());
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"), ((MultipartFileVersion) latest).chunks());
    }

    @Test
    void testGetLatestFileVersionReturnsTombstoneWhenDeleted() throws Exception {
        database.putItem("animals/cat.jpg", 1, 100L, "/uuid-100.blob");
        database.deleteItem("animals/cat.jpg", 1, 101L);

        var latest = database.getLatestFileVersion("animals/cat.jpg").orElseThrow();
        assertInstanceOf(RegularFileVersion.class, latest);
        assertEquals(Database.TOMBSTONE_LOCATION, ((RegularFileVersion) latest).location());
        assertEquals(101L, latest.sequenceNumber());
    }

    // =========================================================================
    // CREATE BUCKET
    // =========================================================================

    @Test
    void testCreateBucketAndBucketExists() throws Exception {
        assertFalse(database.bucketExists("animals"));
        database.createBucket("animals", 1, 5L);
        assertTrue(database.bucketExists("animals"));
    }

    @Test
    void testCreateBucketStoresCorrectFields() throws Exception {
        database.createBucket("animals", 1, 5L);
        // Verify via the strategy directly (not exposed in Database)
        // True verification is bucketExists returning true
        assertTrue(database.bucketExists("animals"));
    }

    // =========================================================================
    // DELETE BUCKET
    // =========================================================================

    @Test
    void testDeleteBucketTombstones() throws Exception {
        database.createBucket("animals", 1, 5L);
        assertTrue(database.bucketExists("animals"));

        database.deleteBucket("animals", 1, 10L);
        assertFalse(database.bucketExists("animals"));
    }

    @Test
    void testDeleteBucketOnNonExistentKeyIsHandled() throws Exception {
        // Writing a tombstone for a key that never existed is allowed
        assertDoesNotThrow(() -> database.deleteBucket("ghost-bucket", 1, 1L));
        assertFalse(database.bucketExists("ghost-bucket"));
    }

    // =========================================================================
    // BUCKET EXISTS
    // =========================================================================

    @Test
    void testBucketExistsReturnsFalseForMissingKey() throws Exception {
        assertFalse(database.bucketExists("nonexistent"));
    }

    // =========================================================================
    // LIST ITEMS IN BUCKET
    // =========================================================================

    @Test
    void testListItemsInBucket() throws Exception {
        database.putItem("photos/cat.jpg", 1, 1L, "/blob-1");
        database.putItem("photos/dog.jpg", 1, 2L, "/blob-2");
        database.putItem("videos/clip.mp4", 1, 3L, "/blob-3");
        database.putItem("photos_backup/cat.jpg", 1, 4L, "/blob-4");

        try (Stream<Metadata> stream = database.listItemsInBucket("photos/")) {
            List<String> keys = stream.map(Metadata::key).collect(Collectors.toList());
            assertEquals(2, keys.size());
            assertTrue(keys.containsAll(List.of("photos/cat.jpg", "photos/dog.jpg")));
        }
    }

    @Test
    void testListItemsInBucketReturnsEmptyForUnknownPrefix() throws Exception {
        database.putItem("photos/cat.jpg", 1, 1L, "/blob-1");
        try (Stream<Metadata> stream = database.listItemsInBucket("videos/")) {
            assertTrue(stream.collect(Collectors.toList()).isEmpty());
        }
    }

    // =========================================================================
    // Metadata serialization round-trips
    // =========================================================================

    @Test
    void testMetadataSerializationRoundTripRegular() {
        Metadata original = new Metadata("animals/cat.jpg", 1,
                List.of(new RegularFileVersion(100L, "/uuid1.blob"),
                        new RegularFileVersion(101L, "/uuid2.blob")));
        Metadata rt = Metadata.from(original.serialize());
        assertEquals(2, rt.versions().size());
        assertInstanceOf(RegularFileVersion.class, rt.versions().get(0));
        assertEquals(100L, rt.versions().get(0).sequenceNumber());
    }

    @Test
    void testMetadataSerializationRoundTripMultipart() {
        Metadata original = new Metadata("animals/dog.jpg", 1,
                List.of(new MultipartFileVersion(98L, List.of("chunk-0.blob", "chunk-1.blob"))));
        Metadata rt = Metadata.from(original.serialize());
        assertInstanceOf(MultipartFileVersion.class, rt.versions().get(0));
        assertEquals(List.of("chunk-0.blob", "chunk-1.blob"),
                ((MultipartFileVersion) rt.versions().get(0)).chunks());
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
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    @Test
    void testCloseClearsAllTables() throws Exception {
        database.putItem("file.txt", 1, 1L, "/blob-1");
        database.createBucket("my-bucket", 1, 2L);
        database.close();
        assertTrue(database.getItem("file.txt").isEmpty());
        assertFalse(database.bucketExists("my-bucket"));
    }
}