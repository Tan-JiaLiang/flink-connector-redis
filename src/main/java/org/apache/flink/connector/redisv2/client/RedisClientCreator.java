package org.apache.flink.connector.redisv2.client;

import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.table.RedisMode;

import java.io.IOException;

public class RedisClientCreator {

    public static RedisClient createClient(RedisOptions options) {
        RedisMode mode = options.getMode();
        switch (mode) {
            case STANDALONE:
                return new StandaloneRedisClient(options);
            case CLUSTER:
                return new ClusterRedisClient(options);
            default:
                throw new UnsupportedOperationException(String.format("mode %s is not support yet.", mode));
        }
    }
}
