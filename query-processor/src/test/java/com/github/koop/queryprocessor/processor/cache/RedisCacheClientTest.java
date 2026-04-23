package com.github.koop.queryprocessor.processor.cache;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link RedisCacheClient} using a Testcontainers-managed
 * Redis instance. The container is started once for the entire test class and
 * shared across all tests.
 *
 * Run with:
 *   mvn test -pl query-processor -Dtest=RedisCacheClientTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
    @Order(1)
    void put_and_get_roundTrip() {
        client.put("test:kv:key", "hello");
        assertEquals("hello", client.get("test:kv:key"));
    }

    @Test
    @Order(2)
    void get_returnsNull_whenKeyAbsent() {
        assertNull(client.get("test:kv:key"));
    }

    @Test
    @Order(3)
    void delete_removesKey() {
        client.put("test:kv:key", "hello");
        client.delete("test:kv:key");
        assertNull(client.get("test:kv:key"));
    }

    @Test
    @Order(4)
    void exists_returnsTrue_whenKeyPresent() {
        client.put("test:kv:key", "hello");
        assertTrue(client.exists("test:kv:key"));
    }

    @Test
    @Order(5)
    void exists_returnsFalse_whenKeyAbsent() {
        assertFalse(client.exists("test:kv:key"));
    }

    @Test
    @Order(6)
    void putWithTTL_keyExpiresAfterTTL() throws InterruptedException {
        client.putWithTTL("test:kv:ttl", "expires", 1L);
        assertEquals("expires", client.get("test:kv:ttl"));
        Thread.sleep(2000);
        assertNull(client.get("test:kv:ttl"));
    }

    @Test
    @Order(7)
    void putIfPresent_returnsFalse_whenKeyAbsent() {
        assertFalse(client.putIfPresent("test:kv:ifpresent", "new-value"));
        assertNull(client.get("test:kv:ifpresent"));
    }

    @Test
    @Order(8)
    void putIfPresent_returnsTrue_andUpdates_whenKeyPresent() {
        client.put("test:kv:ifpresent", "original");
        assertTrue(client.putIfPresent("test:kv:ifpresent", "updated"));
        assertEquals("updated", client.get("test:kv:ifpresent"));
    }

    // ─── Set Tests ────────────────────────────────────────────────────────────

    @Test
    @Order(9)
    void setAdd_and_setMembers_roundTrip() {
        client.setAdd("test:set:basic", "a");
        client.setAdd("test:set:basic", "b");
        client.setAdd("test:set:basic", "c");
        assertEquals(Set.of("a", "b", "c"), client.setMembers("test:set:basic"));
    }

    @Test
    @Order(10)
    void setMembers_returnsEmptySet_whenKeyAbsent() {
        assertTrue(client.setMembers("test:set:basic").isEmpty());
    }

    @Test
    @Order(11)
    void setRemove_returnsTrue_whenMemberRemoved() {
        client.setAdd("test:set:basic", "a");
        assertTrue(client.setRemove("test:set:basic", "a"));
    }

    @Test
    @Order(12)
    void setRemove_returnsFalse_whenMemberAbsent() {
        assertFalse(client.setRemove("test:set:basic", "nonexistent"));
    }

    @Test
    @Order(13)
    void setCreate_setExists_setDelete_lifecycle() {
        assertFalse(client.setExists("test:set:lifecycle"));

        client.setCreate("test:set:lifecycle");
        assertTrue(client.setExists("test:set:lifecycle"));

        assertTrue(client.setMembers("test:set:lifecycle").isEmpty());

        client.setDelete("test:set:lifecycle");
        assertFalse(client.setExists("test:set:lifecycle"));
    }

    @Test
    @Order(14)
    void setCreate_isIdempotent() {
        client.setCreate("test:set:lifecycle");
        client.setCreate("test:set:lifecycle");
        assertTrue(client.setExists("test:set:lifecycle"));
    }

    @Test
    @Order(15)
    void setDelete_removesMarkerAndMembers() {
        client.setCreate("test:set:lifecycle");
        client.setAdd("test:set:lifecycle", "member1");
        client.setDelete("test:set:lifecycle");

        assertFalse(client.setExists("test:set:lifecycle"));
        assertTrue(client.setMembers("test:set:lifecycle").isEmpty());
    }

    @Test
    @Order(16)
    void setAddIfAbsent_returnsFalse_whenSetDoesNotExist() {
        assertFalse(client.setAddIfAbsent("test:set:ifabsent", "a"));
        assertTrue(client.setMembers("test:set:ifabsent").isEmpty());
    }

    @Test
    @Order(17)
    void setAddIfAbsent_returnsTrue_whenSetExistsAndMemberNew() {
        client.setCreate("test:set:ifabsent");
        assertTrue(client.setAddIfAbsent("test:set:ifabsent", "a"));
        assertTrue(client.setMembers("test:set:ifabsent").contains("a"));
    }

    @Test
    @Order(18)
    void setAddIfAbsent_returnsFalse_whenMemberAlreadyPresent() {
        client.setCreate("test:set:ifabsent");
        client.setAdd("test:set:ifabsent", "a");
        assertFalse(client.setAddIfAbsent("test:set:ifabsent", "a"));
    }

    @Test
    @Order(19)
    void setAddIfPresent_returnsFalse_whenSetDoesNotExist() {
        assertFalse(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertTrue(client.setMembers("test:set:ifpresent").isEmpty());
    }

    @Test
    @Order(20)
    void setAddIfPresent_returnsTrue_andAddsMember_whenSetExists() {
        client.setCreate("test:set:ifpresent");
        assertTrue(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertTrue(client.setMembers("test:set:ifpresent").contains("a"));
    }

    @Test
    @Order(21)
    void setAddIfPresent_addsAgain_whenMemberAlreadyPresent() {
        client.setCreate("test:set:ifpresent");
        client.setAdd("test:set:ifpresent", "a");
        assertTrue(client.setAddIfPresent("test:set:ifpresent", "a"));
        assertEquals(1, client.setMembers("test:set:ifpresent").size());
    }

    @Test
    @Order(22)
    void setAdd_createsExistenceMarker() {
        // setAdd should make setExists return true even without an explicit setCreate
        assertFalse(client.setExists("test:set:basic"));
        client.setAdd("test:set:basic", "a");
        assertTrue(client.setExists("test:set:basic"));
    }
}