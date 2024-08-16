package org.apache.flink.connector.redisv2.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.options.RedisWriteOptions;
import org.apache.flink.connector.redisv2.sink.RedisDynamicTableSink;
import org.apache.flink.connector.redisv2.command.RedisCommand;
import org.apache.flink.connector.redisv2.source.RedisDynamicTableSource;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.redisv2.table.RedisConnectorOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Redis connector factory
 */
@Internal
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis-v2";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        RedisCommand command = tableOptions.get(COMMAND);
        if (command == null) {
            throw new IllegalArgumentException(String.format("%s must be set when sink", COMMAND.key()));
        }

        String putScriptPath = tableOptions.get(SINK_PUT_LUA_SCRIPT_PATH);
        String delScriptPath = tableOptions.get(SINK_DEL_LUA_SCRIPT_PATH);

        RedisOptions options = toRedisOptions(tableOptions, false);
        RedisWriteOptions writeOptions = toRedisWriteOptions(tableOptions);

        return new RedisDynamicTableSink(command, options, writeOptions, schema, putScriptPath, delScriptPath);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        boolean async = Boolean.TRUE.equals(tableOptions.get(LOOKUP_ASYNC));

        RedisCommand command = tableOptions.get(COMMAND);
        if (command == null) {
            throw new IllegalArgumentException(String.format("%s must be set when lookup", COMMAND.key()));
        }

        LookupCache cache = null;
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }

        Integer maxRetries = tableOptions.get(LookupOptions.MAX_RETRIES);
        Duration backoff = tableOptions.get(LOOKUP_RETRY_BACKOFF);

        RedisOptions options = toRedisOptions(tableOptions, true);

        return new RedisDynamicTableSource(options, command, schema, async, maxRetries, backoff, cache);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(MODE);
        set.add(NODES);
        set.add(COMMAND);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(IO_POOL_SIZE);
        set.add(COMPUTATION_POOL_SIZE);
        set.add(TIMEOUT);
        set.add(PASSWORD);
        set.add(SINK_PUT_LUA_SCRIPT_PATH);
        set.add(SINK_DEL_LUA_SCRIPT_PATH);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_RETRY_BACKOFF);
        set.add(LookupOptions.CACHE_TYPE);
        set.add(LookupOptions.MAX_RETRIES);
        set.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        set.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        set.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        set.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        return set;
    }

    public RedisOptions toRedisOptions(ReadableConfig config, boolean autoFlushCommands) {
        return new RedisOptions.Builder()
                .setMode(config.get(MODE))
                .setNodes(config.get(NODES))
                .setPassword(config.get(PASSWORD))
                .setTimeout(config.get(TIMEOUT))
                .setIoThreadPoolSize(config.get(IO_POOL_SIZE))
                .setComputationThreadPoolSize(config.get(COMPUTATION_POOL_SIZE))
                .setAutoFlushCommands(autoFlushCommands)
                .build();
    }

    public RedisWriteOptions toRedisWriteOptions(ReadableConfig config) {
        return new RedisWriteOptions.Builder()
                .setBufferFlushIntervalMillis(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .setBufferFlushMaxRows(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .setTimeout(config.get(TIMEOUT))
                .setParallelism(config.get(SINK_PARALLELISM))
                .build();
    }
}
