package com.github.koop.queryprocessor.processor.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.net.URI;
import java.util.Set;

/**
 * Redis-backed {@link CacheClient} for production multi-QP deployments.
 *
 * <p>Backed by a {@link JedisPool} constructed from the {@code REDIS_URL}
 * environment variable (e.g. {@code redis://redis-master:6379}). Falls back
 * to {@code redis://localhost:6379} if the variable is not set.
 *
 * <p>Set semantics notes:
 * <ul>
 *   <li>{@link #setCreate} records set existence via a separate marker key
 *       ({@code __set_exists__:{key}}) because Redis sets have no concept of
 *       an empty set — they cease to exist when their last member is removed.
 *       The marker is cleaned up by {@link #setDelete}.</li>
 *   <li>{@link #setExists} checks the marker key, not the set itself.</li>
 *   <li>{@link #setAddIfAbsent} requires the set to exist (marker present)
 *       and adds the member only if it is not already in the set.</li>
 *   <li>{@link #setAddIfPresent} requires the set to exist (marker present)
 *       and adds the member unconditionally.</li>
 * </ul>
 *
 * <p>All methods wrap Redis/IO errors in an unchecked
 * {@link RuntimeException} so callers do not need to handle checked exceptions.
 */
public class RedisCacheClient implements CacheClient {

    private static final Logger logger = LogManager.getLogger(RedisCacheClient.class);

    /** Prefix used to track whether a logical set has been created. */
    private static final String SET_EXISTS_PREFIX = "__set_exists__:";

    private final JedisPool pool;

    // ─── Construction ─────────────────────────────────────────────────────────

    /**
     * Creates a client using the {@code REDIS_URL} environment variable.
     * Falls back to {@code redis://localhost:6379} if not set.
     */
    public RedisCacheClient() {
        this(System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379"));
    }

    /**
     * Creates a client connecting to the given Redis URL.
     *
     * @param redisUrl e.g. {@code redis://redis-master:6379}
     */
    public RedisCacheClient(String redisUrl) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(32);
        config.setMaxIdle(8);
        config.setMinIdle(2);
        config.setTestOnBorrow(true);
        this.pool = new JedisPool(config, URI.create(redisUrl));
        logger.info("RedisCacheClient connected to {}", redisUrl);
    }

    /** Package-private constructor for testing with a pre-built pool. */
    RedisCacheClient(JedisPool pool) {
        this.pool = pool;
    }

    // ─── Key-Value Operations ─────────────────────────────────────────────────

    @Override
    public void put(String key, String value) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Redis PUT failed for key: " + key, e);
        }
    }

    @Override
    public void putWithTTL(String key, String value, long ttlSeconds) {
        try (Jedis jedis = pool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
        } catch (Exception e) {
            throw new RuntimeException("Redis SETEX failed for key: " + key, e);
        }
    }

    /**
     * Updates {@code key} only if it already exists (Redis {@code SET XX}).
     *
     * @return {@code true} if the key existed and was updated;
     *         {@code false} if the key did not exist.
     */
    @Override
    public boolean putIfPresent(String key, String value) {
        try (Jedis jedis = pool.getResource()) {
            // SET key value XX — only set if key already exists
            String result = jedis.set(key, value, SetParams.setParams().xx());
            return "OK".equals(result);
        } catch (Exception e) {
            throw new RuntimeException("Redis SET XX failed for key: " + key, e);
        }
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            throw new RuntimeException("Redis GET failed for key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            throw new RuntimeException("Redis DEL failed for key: " + key, e);
        }
    }

    @Override
    public boolean exists(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.exists(key);
        } catch (Exception e) {
            throw new RuntimeException("Redis EXISTS failed for key: " + key, e);
        }
    }

    // ─── Set Operations ───────────────────────────────────────────────────────

    /**
     * Adds {@code member} to the set at {@code key}, creating it if absent.
     * Does not require the set to have been created via {@link #setCreate}.
     */
    @Override
    public void setAdd(String key, String member) {
        try (Jedis jedis = pool.getResource()) {
            jedis.sadd(key, member);
        } catch (Exception e) {
            throw new RuntimeException("Redis SADD failed for key: " + key, e);
        }
    }

    /**
     * Atomically adds {@code member} to the set at {@code key} only if the set
     * exists (marker present) and the member is not already in the set.
     *
     * <p>Implemented as a Lua script to guarantee atomicity — the marker check
     * and SADD execute as a single Redis operation with no race window.
     *
     * @return {@code true} if the set existed and the member was newly added;
     *         {@code false} if the set did not exist or the member was already present.
     */
    @Override
    public boolean setAddIfAbsent(String key, String member) {
        // KEYS[1] = marker key, KEYS[2] = set key, ARGV[1] = member
        // Returns 1 if added, 0 if set doesn't exist or member already present
        String script = """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    return 0
                end
                return redis.call('SADD', KEYS[2], ARGV[1])
                """;
        try (Jedis jedis = pool.getResource()) {
            Object result = jedis.eval(script, 2, setExistsKey(key), key, member);
            return Long.valueOf(1L).equals(result);
        } catch (Exception e) {
            throw new RuntimeException("Redis setAddIfAbsent failed for key: " + key, e);
        }
    }

    /**
     * Atomically adds {@code member} to the set at {@code key} only if the set
     * exists (marker present). The member is added unconditionally if the set exists.
     *
     * <p>Implemented as a Lua script to guarantee atomicity — the marker check
     * and SADD execute as a single Redis operation with no race window.
     *
     * @return {@code true} if the set existed (member was added);
     *         {@code false} if the set did not exist.
     */
    @Override
    public boolean setAddIfPresent(String key, String member) {
        // KEYS[1] = marker key, KEYS[2] = set key, ARGV[1] = member
        // Returns 1 if set existed (and member was added), 0 if set doesn't exist
        String script = """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    return 0
                end
                redis.call('SADD', KEYS[2], ARGV[1])
                return 1
                """;
        try (Jedis jedis = pool.getResource()) {
            Object result = jedis.eval(script, 2, setExistsKey(key), key, member);
            return Long.valueOf(1L).equals(result);
        } catch (Exception e) {
            throw new RuntimeException("Redis setAddIfPresent failed for key: " + key, e);
        }
    }

    /**
     * Removes {@code member} from the set at {@code key}.
     *
     * @return {@code true} if the member existed and was removed;
     *         {@code false} if the set did not exist or the member was not present.
     */
    @Override
    public boolean setRemove(String key, String member) {
        try (Jedis jedis = pool.getResource()) {
            // SREM returns the number of members removed
            return jedis.srem(key, member) == 1L;
        } catch (Exception e) {
            throw new RuntimeException("Redis SREM failed for key: " + key, e);
        }
    }

    /**
     * Creates a logical empty set at {@code key} by writing a marker key.
     * Subsequent calls to {@link #setExists}, {@link #setAddIfAbsent}, and
     * {@link #setAddIfPresent} will treat this set as existing even while it
     * has no members.
     */
    @Override
    public void setCreate(String key) {
        try (Jedis jedis = pool.getResource()) {
            // SET NX so repeated calls are idempotent
            jedis.set(setExistsKey(key), "1", SetParams.setParams().nx());
        } catch (Exception e) {
            throw new RuntimeException("Redis setCreate failed for key: " + key, e);
        }
    }

    /**
     * Returns {@code true} if the set was previously created via
     * {@link #setCreate} and not yet deleted via {@link #setDelete}.
     */
    @Override
    public boolean setExists(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.exists(setExistsKey(key));
        } catch (Exception e) {
            throw new RuntimeException("Redis setExists failed for key: " + key, e);
        }
    }

    @Override
    public Set<String> setMembers(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.smembers(key);
        } catch (Exception e) {
            throw new RuntimeException("Redis SMEMBERS failed for key: " + key, e);
        }
    }

    /**
     * Deletes the set and its existence marker.
     */
    @Override
    public void setDelete(String key) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key, setExistsKey(key));
        } catch (Exception e) {
            throw new RuntimeException("Redis setDelete failed for key: " + key, e);
        }
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────────

    /**
     * Closes the underlying connection pool. Call on application shutdown.
     */
    public void close() {
        pool.close();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static String setExistsKey(String key) {
        return SET_EXISTS_PREFIX + key;
    }
}