package org.apache.flink.connector.redisv2.client;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;

import java.io.IOException;

public interface RedisClient {

    RedisFuture<String> set(String key, String value, SetArgs args);

    RedisFuture<Long> del(String key);

    RedisFuture<String> get(String key);

    RedisFuture<Boolean> hset(String key, String field, String value);

    RedisFuture<Boolean> hsetnx(String key, String field, String value);

    RedisFuture<Long> hdel(String key, String field);

    RedisFuture<String> hget(String key, String field);

    RedisFuture<Long> incrby(String key, long amount);

    RedisFuture<Long> decrby(String key, long amount);

    RedisFuture<Boolean> expire(String key, Long expire);

    void flush() throws IOException;

    void close() throws IOException;
}
