package org.apache.flink.connector.redisv2.client;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

public class StandaloneRedisClient implements RedisClient {

    private final io.lettuce.core.RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;

    public StandaloneRedisClient(RedisOptions options) {
        String nodes = options.getNodes();
        String[] nodeSplit = nodes.split(":");
        if (nodeSplit.length != 2) {
            throw new IllegalArgumentException("2 more nodes found in standalone mode");
        }
        String host = nodeSplit[0];
        int port = Integer.parseInt(nodeSplit[1]);

        RedisURI.Builder builder =
                RedisURI.builder()
                        .withHost(host)
                        .withPort(port)
                        .withTimeout(options.getTimeout());
        if (!StringUtils.isNullOrWhitespaceOnly(options.getPassword())) {
            builder.withPassword(options.getPassword().toCharArray());
        }
        RedisURI uri = builder.build();

        DefaultClientResources resources =
                DefaultClientResources.builder()
                        .ioThreadPoolSize(options.getIoThreadPoolSize())
                        .computationThreadPoolSize(options.getComputationThreadPoolSize())
                        .build();

        this.client = io.lettuce.core.RedisClient.create(resources, uri);
        this.connection = client.connect();
        this.connection.setAutoFlushCommands(options.isAutoFlushCommands());
        this.commands = connection.async();
    }

    @Override
    public RedisFuture<String> set(String key, String value, SetArgs args) {
        return commands.set(key, value, args);
    }

    @Override
    public RedisFuture<Long> del(String key) {
        return commands.del(key);
    }

    @Override
    public RedisFuture<String> get(String key) {
        return commands.get(key);
    }

    @Override
    public RedisFuture<Boolean> hset(String key, String field, String value) {
        return commands.hset(key, field, value);
    }

    @Override
    public RedisFuture<Boolean> hsetnx(String key, String field, String value) {
        return commands.hsetnx(key, field, value);
    }

    @Override
    public RedisFuture<Long> hdel(String key, String field) {
        return commands.hdel(key, field);
    }

    @Override
    public RedisFuture<String> hget(String key, String field) {
        return commands.hget(key, field);
    }

    @Override
    public RedisFuture<Long> incrby(String key, long amount) {
        return commands.incrby(key, amount);
    }

    @Override
    public RedisFuture<Long> decrby(String key, long amount) {
        return commands.decrby(key, amount);
    }

    @Override
    public RedisFuture<Boolean> expire(String key, Long expire) {
        return commands.expire(key, expire);
    }

    @Override
    public void flush() throws IOException {
        connection.flushCommands();
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
