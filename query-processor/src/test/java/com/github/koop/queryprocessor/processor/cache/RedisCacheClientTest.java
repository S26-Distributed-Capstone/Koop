package com.github.koop.queryprocessor.processor.cache;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link RedisCacheClient} using a Testcontainers-managed
 * Redis instance. The container is shared across the class; each test cleans up
 * its own keys in {@link #cleanUp()} so tests are independent.
 *
 * Run with:
 *   mvn test -pl query-processor -Dtest=RedisCacheClientTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RedisCacheClientTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private RedisCacheClient client;

    @BeforeAll
    void setUp() {
        String url = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        client = new RedisCacheClient(url);
    }

    @AfterAll
    void tearDown() {
        client.close();
    }

    @BeforeEach
    void cleanUp() {
        client.delete("test:kv:key");
        client.delete("test:kv:ttl");
        client.delete("test:kv:ifpresent");
        client.setDelete("test:set:basic");
        client.setDelete("test:set:lifecycle");
        client.setDelete("test:set:ifabsent");
        client.setDelete("test:set:ifpresent");
    }

    // ─── Key-Value Tests ──────────────────────────────────────────────────────

    @Test
    void put_and_get_roundTrip() {
        client.put("test:kv:key", "hello");
        assertEquals("hello", client.get("test:kv:key"));
    }

    @Test
    void get_returnsNull_whenKeyAbsent() {
        assertNull(client.get("test:kv:key"));
    }

    @Test
    void delete_removesKey() {
        client.put("test:kv:key", "hello");
        client.delete("test:kv:key");
        assertNull(client.get("test:kv:key"));
    }

    @Test
    void exists_returnsTrue_whenKeyPresent() {
        client.put("test:kv:key", "hello");
        assertTrue(client.exists("test:kv:key"));
    }

    @Test
    void exists_returnsFalse_whenKeyAbsent() {
        assertFalse(client.exists("test:kv:key"));
    }

    @Test
    void putWithTTL_keyExpiresAfterTTL() {
        client.putWithTTL("test:kv:ttl", "expires", 1L);
        assertEquals("expires", client.get("test:kv:ttl"));
        await()
            .atMost(Duration.ofSeconds(3))
            .pollInterval(Duration.ofMillis(100))
            .until(() -> client.get("test:kv:ttl") == null);
    }

    @Test
    void putIfPresent_returnsFalse_whenKeyAbsent() {
        assertFalse(client.putIfPresent("test:kv:ifpresent", "new-value"));
        assertNull(client.get("test:kv:ifpresent"));
    }

    @Test
    void putIfPresent_returnsTrue_andUpdates_whenKeyPresent() {
        client.put("test:kv:ifpresent", "original");
        assertTrue(client.putIfPresent("test:kv:ifpresent", "updated"));
        assertEquals("updated", client.get("test:kv:ifpresent"));
    }

    // ─── Set Tests ────────────────────────────────────────────────────────────

    @Test
    void setAdd_and_setMembers_roundTrip() {
        client.setAdd("test:set:basic", "a");
        client.setAdd("test:set:basic", "b");
        client.setAdd("test:set:basic", "c");
        assertEquals(Set.of("a", "b", "c"), client.setMembers("test:set:basic"));
    }

    @Test
    void setMembers_returnsEmptySet_whenKeyAbsent() {
        assertTrue(client.setMembers("test:set:basic").isEmpty());
    }

    @Test
    void setRemove_returnsTrue_whenMemberRemoved() {
        client.setAdd("test:set:basic", "a");
        assertTrue(client.setRemove("test:set:basic", "a"));
    }

    @Test
    void setRemove_returnsFalse_whenMemberAbsent() {
        assertFalse(client.setRemove("test:set:basic", "nonexistent"));
    }

    @Test
    void setCreate_setExists_setDelete_lifecycle() {
        assertFalse(client.setExists("test:set:lifecycle"));

        client.setCreate("test:set:lifecycle");
        assertTrue(client.setExists("test:set:lifecycle"));

        assertTrue(client.setMembers("test:set:lifecycle").isEmpty());

        client.setDelete("test:set:lifecycle");
        assertFalse(client.setExists("test:set:lifecycle"));
    }

    @Test
    void setCreate_isIdempotent() {
        client.setCreate("test:set:lifecycle");
        client.setCreate("test:set:lifecycle");
        assertTrue(client.setExists("test:set:lifecycle"));
    }

    @Test
    void setDelete_removesMarkerAndMembers() {
        client.setCreate("test:set:lifecycle");
        client.setAdd("test:set:lifecycle", "member1");
        client.setDelete("test:set:lifecycle");

        assertFalse(client.setExists("test:set:lifecycle"));
        assertTrue(client.setMembers("test:set:lifecycle").isEmpty());
    }

    @Test
    void setAddIfAbsent_returnsFalse_whenSetDoesNotExist() {
        assertFalse(client.setAddIfAbsent("test:set:ifabsent", "a"));
        assertTrue(client.setMembers("test:set:ifabsent").isEmpty());
    }

    @Test
    void setAddIfAbsent_returnsTrue_whenSetExistsAndMemberNew() {
        client.setCreate("test:set:ifabsent");
        assertTrue(client.setAddIfAbsent("test:set:ifabsent", "a"));
        assertTrue(client.setMembers("test:set:ifabsent").contains("a"));
    }

    @Test
    void setAddIfAbsent_returnsFalse_whenMemberAlreadyPresent() {
        client.setCreate("test:set:ifabsent");
        client.setAdd("test:set:ifabsent", "a");
        assertFalse(client.setAddIfAbsent("test:set:ifabsent", "a"));
    }

    @Test
    void setAddIfPresent_returnsFalse_whenSetDoesNotExist() {
        assertFalse(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertTrue(client.setMembers("test:set:ifpresent").isEmpty());
    }

    @Test
    void setAddIfPresent_returnsTrue_andAddsMember_whenSetExists() {
        client.setCreate("test:set:ifpresent");
        assertTrue(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertTrue(client.setMembers("test:set:ifpresent").contains("a"));
    }

    @Test
    void setAddIfPresent_addsAgain_whenMemberAlreadyPresent() {
        client.setCreate("test:set:ifpresent");
        client.setAdd("test:set:ifpresent", "a");
        assertTrue(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertEquals(1, client.setMembers("test:set:ifpresent").size());
    }

    @Test
    void setAdd_createsExistenceMarker() {
        // setAdd should make setExists return true even without an explicit setCreate
        assertFalse(client.setExists("test:set:basic"));
        client.setAdd("test:set:basic", "a");
        assertTrue(client.setExists("test:set:basic"));
    }
}
