package org.apache.flink.connector.redisv2;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.client.RedisClientCreator;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.table.RedisMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class TestRowData {

    @Test
    public void test() {
        GenericRowData input = GenericRowData.of(1, StringData.fromString("hello"));
        System.out.println(input.getInt(0));
        System.out.println(((StringData) input.getString(1)).toString());
    }

    @Test
    public void test2() throws ExecutionException, InterruptedException {
        RedisOptions options = RedisOptions.builder()
                .setMode(RedisMode.STANDALONE)
                .setIoThreadPoolSize(1)
                .setComputationThreadPoolSize(1)
                .setTimeout(Duration.ofSeconds(30))
                .setNodes("localhost:51375")
                .build();

        RedisClient client = RedisClientCreator.createClient(options);
        RedisFuture<String> future = client.get("1");

        System.out.println(future.get());
    }

    @Test
    public void test03() {
        RedisURI uri =
                RedisURI.builder()
                        .withHost("localhost")
                        .withPort(51375)
                        .withTimeout(Duration.ofSeconds(500))
                        .build();

        DefaultClientResources resources =
                DefaultClientResources.builder()
                        .ioThreadPoolSize(5)
                        .computationThreadPoolSize(5)
                        .build();

        try (io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create(resources, uri)) {
            StatefulRedisConnection<String, String> connection = client.connect();
            RedisCommands<String, String> commands = connection.sync();
            String value = commands.get("1");

            System.out.println(value);

            RedisAsyncCommands<String, String> asyncCommands = connection.async();
            String asyncValue = asyncCommands.get("1").get();
            System.out.println(asyncValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
