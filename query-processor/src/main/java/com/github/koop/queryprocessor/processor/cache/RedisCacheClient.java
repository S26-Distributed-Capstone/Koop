package com.github.koop.queryprocessor.processor.cache;

import java.util.Set;

/**
 * Redis-backed CacheClient for production use.
 * Required for multi-QP deployments where different QP instances handle
 * different parts of the same multipart upload.
 *
 * Dependency (not yet added to pom.xml):
 *   io.lettuce:lettuce-core OR redis.clients:jedis
 *
 * All methods throw UnsupportedOperationException until implemented.
 */
public class RedisCacheClient implements CacheClient {

    // TODO: inject Redis client / connection pool.

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException("RedisCacheClient.put not implemented");
    }

    @Override
    public String get(String key) {
        throw new UnsupportedOperationException("RedisCacheClient.get not implemented");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("RedisCacheClient.delete not implemented");
    }

    @Override
    public boolean exists(String key) {
        throw new UnsupportedOperationException("RedisCacheClient.exists not implemented");
    }

    @Override
    public void setAdd(String key, String member) {
        throw new UnsupportedOperationException("RedisCacheClient.setAdd not implemented");
    }

    @Override
    public Set<String> setMembers(String key) {
        throw new UnsupportedOperationException("RedisCacheClient.setMembers not implemented");
    }

    @Override
    public void setDelete(String key) {
        throw new UnsupportedOperationException("RedisCacheClient.setDelete not implemented");
    }
}
