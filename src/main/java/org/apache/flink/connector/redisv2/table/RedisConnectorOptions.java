/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redisv2.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.redisv2.command.RedisCommand;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

/**
 * Options for the Redis connector.
 */
@PublicEvolving
public class RedisConnectorOptions {

    public static final ConfigOption<RedisMode> MODE =
            ConfigOptions.key("mode")
                    .enumType(RedisMode.class)
                    .noDefaultValue()
                    .withDescription("Redis deploy mode, support standalone and cluster");

    public static final ConfigOption<RedisCommand> COMMAND =
            ConfigOptions.key("command")
                    .enumType(RedisCommand.class)
                    .noDefaultValue()
                    .withDescription("Redis command");

    public static final ConfigOption<String> NODES =
            ConfigOptions.key("nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redis nodes, example: 192.168.1.1:6379,192.168.1.2:6379");

    public static final ConfigOption<Integer> IO_POOL_SIZE =
            ConfigOptions.key("io.pool.size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Lettuce i/o thread pool size");

    public static final ConfigOption<Integer> COMPUTATION_POOL_SIZE =
            ConfigOptions.key("computation.pool.size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Lettuce computation thread pool size");

    public static final ConfigOption<Duration> TIMEOUT =
            ConfigOptions.key("timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Lettuce execute command timeout");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redis password");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup.");

    public static final ConfigOption<Duration> LOOKUP_RETRY_BACKOFF =
            ConfigOptions.key("lookup.retry.backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Redis lookup retry backoff");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Redis write batch size");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("Redis write batch interval");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    private RedisConnectorOptions() {
    }
}
