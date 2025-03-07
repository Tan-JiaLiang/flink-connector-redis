package org.apache.flink.connector.redisv2.client;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ClusterRedisClient implements RedisClient {

    private final RedisClusterClient client;
    private final StatefulRedisClusterConnection<String, String> connection;
    private final RedisAdvancedClusterAsyncCommands<String, String> commands;

    public ClusterRedisClient(RedisOptions options) {
        List<RedisURI> uris = new ArrayList<>();
        String[] nodes = options.getNodes().split(",");
        for (String node : nodes) {
            String[] split = node.split(":");
            String host = split[0];
            int port = Integer.parseInt(split[1]);
            RedisURI.Builder builder =
                    RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withTimeout(options.getTimeout());
            if (!StringUtils.isNullOrWhitespaceOnly(options.getPassword())) {
                builder.withPassword(options.getPassword().toCharArray());
            }
            RedisURI uri = builder.build();
            uris.add(uri);
        }

        DefaultClientResources resources =
                DefaultClientResources.builder()
                        .ioThreadPoolSize(options.getIoThreadPoolSize())
                        .computationThreadPoolSize(options.getComputationThreadPoolSize())
                        .build();

        client = RedisClusterClient.create(resources, uris);
        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enableAdaptiveRefreshTrigger(
                                ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                                ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10L))
                        .build();
        client.setOptions(
                ClusterClientOptions.builder()
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .build());

        connection = client.connect();
        connection.setAutoFlushCommands(options.isAutoFlushCommands());
        commands = connection.async();
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
        return commands.hdel(key);
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
    public RedisFuture<String> eval(String script, ScriptOutputType type, String[] keys, String[] values) {
        return commands.eval(script, ScriptOutputType.STATUS, keys, values);
    }

    @Override
    public void flush() throws IOException {
        connection.flushCommands();
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.flushCommands();
            connection.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
