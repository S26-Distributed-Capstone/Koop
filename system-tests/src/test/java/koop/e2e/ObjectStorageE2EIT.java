package koop.e2e;

import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.UUID;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Full E2E tests for the S3-compatible HTTP API.
 * These tests never import internal KoopDB classes — they only speak HTTP.
 * They validate the entire stack: HTTP → Query Processor → erasure coding
 * → binary TCP → Storage Nodes → etcd/Redis.
 */
class ObjectStorageE2EIT extends BaseE2EIT {

    // ---------------------------------------------------------------
    // Health check
    // ---------------------------------------------------------------

    @Test
    void healthEndpointReturns200OnAllQueryProcessors() {
        when().get("/health").then().statusCode(200);

        // Also verify the other two replicas are up
        given().baseUri(KoopCluster.QP_BASE_URL_2)
            .when().get("/health").then().statusCode(200);

        given().baseUri(KoopCluster.QP_BASE_URL_3)
            .when().get("/health").then().statusCode(200);
    }

    // ---------------------------------------------------------------
    // Basic CRUD
    // ---------------------------------------------------------------

    @Test
    void putThenGetReturnsIdenticalBytes() {
        byte[] payload = randomBytes(1024); // 1 KB
        String key = uniqueKey();

        given()
            .contentType(ContentType.BINARY)
            .body(payload)
        .when()
            .put("/videos/" + key)
        .then()
            .statusCode(anyOf(is(200), is(201)));

        byte[] retrieved = when()
            .get("/videos/" + key)
        .then()
            .statusCode(200)
            .extract().asByteArray();

        assertArrayEquals(payload, retrieved,
            "Downloaded bytes must exactly match what was uploaded");
    }

    @Test
    void deleteRemovesObjectAndReturns404OnSubsequentGet() {
        byte[] payload = "temporary object".getBytes();
        String key = uniqueKey();

        given().body(payload).put("/videos/" + key);

        when().delete("/videos/" + key)
            .then().statusCode(anyOf(is(200), is(204)));

        when().get("/videos/" + key)
            .then().statusCode(404);
    }

    @Test
    void overwritingKeyReturnsMostRecentContent() {
        String key = uniqueKey();

        given().body("version-one".getBytes()).put("/videos/" + key);
        given().body("version-two".getBytes()).put("/videos/" + key);

        byte[] result = when().get("/videos/" + key)
            .then().statusCode(200)
            .extract().asByteArray();

        assertArrayEquals("version-two".getBytes(), result,
            "Second PUT should overwrite the first");
    }

    // ---------------------------------------------------------------
    // Large file — crosses 1 MB shard boundaries, exercises
    // the full Reed-Solomon stripe pipeline and zero-copy streaming
    // ---------------------------------------------------------------

    @Test
    void largeFileRoundTripIntact_crossesShardBoundaries() {
        byte[] payload = randomBytes(15 * 1024 * 1024); // 15 MB — same size as RealStorageNodesIT
        String key = uniqueKey();

        given()
            .contentType(ContentType.BINARY)
            .body(payload)
        .when()
            .put("/videos/" + key)
        .then()
            .statusCode(anyOf(is(200), is(201)));

        byte[] retrieved = when()
            .get("/videos/" + key)
        .then()
            .statusCode(200)
            .extract().asByteArray();

        assertArrayEquals(payload, retrieved,
            "15 MB file must survive full erasure encode/decode pipeline intact");
    }

    // ---------------------------------------------------------------
    // Multi-replica query processor — same object accessible from any QP
    // ---------------------------------------------------------------

    @Test
    void objectWrittenToOneQPIsReadableFromAnotherQP() {
        byte[] payload = randomBytes(512);
        String key = uniqueKey();

        // Write via QP replica 1
        given()
            .baseUri(KoopCluster.QP_BASE_URL)
            .body(payload)
            .put("/videos/" + key)
        .then()
            .statusCode(anyOf(is(200), is(201)));

        // Read back from QP replica 2 — tests that routing is consistent
        byte[] retrieved = given()
            .baseUri(KoopCluster.QP_BASE_URL_2)
            .when().get("/videos/" + key)
        .then()
            .statusCode(200)
            .extract().asByteArray();

        assertArrayEquals(payload, retrieved,
            "Any Query Processor replica should be able to serve any object");
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static String uniqueKey() {
        return "e2e-" + UUID.randomUUID() + ".bin";
    }

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        new SecureRandom().nextBytes(b);
        return b;
    }
}