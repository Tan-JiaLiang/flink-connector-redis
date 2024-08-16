package org.apache.flink.connector.redisv2;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LettuceTest extends RedisTestingClusterAutoStarter {

    @Test
    void testLettuce() {
        RedisURI uri =
                RedisURI.builder()
                        .withHost(getRedisHost())
                        .withPort(getRedisPort())
                        .withTimeout(Duration.ofSeconds(500))
                        .build();

        DefaultClientResources resources =
                DefaultClientResources.builder()
                        .ioThreadPoolSize(5)
                        .computationThreadPoolSize(5)
                        .build();

        List<RedisFuture<?>> futures = new ArrayList<>();
        try (RedisClient redisClient = RedisClient.create(resources, uri)) {
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            connection.setAutoFlushCommands(false);
            RedisAsyncCommands<String, String> commands = connection.async();

            futures.add(commands.set("hello", "world"));
            futures.add(commands.set("h2", "w2"));
            futures.add(commands.exists("hello", "h2"));

            connection.flushCommands();

            boolean timeout =
                    LettuceFutures.awaitAll(
                            10, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
            if (!timeout) {
                throw new RuntimeException("batch write timeout");
            }

            connection.setAutoFlushCommands(true);

            System.out.println(commands.get("hello").get());
            System.out.println(commands.get("h2").get());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLua() throws Exception{
        RedisURI uri =
                RedisURI.builder()
                        .withHost(getRedisHost())
                        .withPort(getRedisPort())
                        .withTimeout(Duration.ofSeconds(500))
                        .build();

        DefaultClientResources resources =
                DefaultClientResources.builder()
                        .ioThreadPoolSize(5)
                        .computationThreadPoolSize(5)
                        .build();

        String script = "redis.call('SET', KEYS[1], ARGV[1])";

        try (RedisClient redisClient = RedisClient.create(resources, uri)) {
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisAsyncCommands<String, String> commands = connection.async();

            RedisFuture<Object> eval = commands.eval(script, ScriptOutputType.STATUS, new String[]{"k1"}, "v1");
            Object status = eval.get();
            System.out.println(status);

            System.out.println(commands.get("k1").get());
        }
    }
}
