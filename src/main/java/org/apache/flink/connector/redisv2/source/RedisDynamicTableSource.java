package org.apache.flink.connector.redisv2.source;

import org.apache.flink.connector.redisv2.command.RedisCommand;
import org.apache.flink.connector.redisv2.command.RedisRowDataCommandFactory;
import org.apache.flink.connector.redisv2.command.read.LookupCommandExec;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableLookupFunctionDelegator;

import javax.annotation.Nullable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;

public class RedisDynamicTableSource implements LookupTableSource {

    private final RedisOptions options;
    private final RedisCommand lookupCommand;
    private final ResolvedSchema schema;
    private final boolean async;
    private final int maxRetryTimes;
    private final Duration retryBackoff;
    private final LookupCache cache;

    public RedisDynamicTableSource(RedisOptions options,
                                   RedisCommand lookupCommand,
                                   ResolvedSchema schema,
                                   boolean async,
                                   Integer maxRetryTimes,
                                   Duration retryBackoff,
                                   @Nullable LookupCache cache) {
        this.options = options;
        this.lookupCommand = lookupCommand;
        this.schema = schema;
        this.async = async;
        this.maxRetryTimes = maxRetryTimes;
        this.retryBackoff = retryBackoff;
        this.cache = cache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        checkArgument(context.getKeys().length == schema.getPrimaryKeyIndexes().length,
                "Redis must use all the primary key for lookup.");
        LookupCommandExec<RowData> command =
                RedisRowDataCommandFactory.createLookupCommandExec(lookupCommand, options, schema);
        if (async) {
            AsyncLookupFunction lookupFunction = new RedisRowDataAsyncLookupFunction(command);
            if (maxRetryTimes > 0) {
                ResultRetryStrategy strategy =
                        ResultRetryStrategy.fixedDelayRetry(
                                maxRetryTimes,
                                retryBackoff.toMillis(),
                                RetryPredicates.EMPTY_RESULT_PREDICATE);
                lookupFunction = new RetryableAsyncLookupFunctionDelegator(lookupFunction, strategy);
            }

            if (cache != null) {
                return PartialCachingAsyncLookupProvider.of(lookupFunction, cache);
            } else {
                return AsyncLookupFunctionProvider.of(lookupFunction);
            }
        } else {
            LookupFunction lookupFunction = new RedisRowDataLookupFunction(command);
            if (maxRetryTimes > 0) {
                ResultRetryStrategy strategy =
                        ResultRetryStrategy.fixedDelayRetry(
                                maxRetryTimes,
                                retryBackoff.toMillis(),
                                RetryPredicates.EMPTY_RESULT_PREDICATE);
                lookupFunction = new RetryableLookupFunctionDelegator(lookupFunction, strategy);
            }

            if (cache != null) {
                return PartialCachingLookupProvider.of(lookupFunction, cache);
            } else {
                return LookupFunctionProvider.of(lookupFunction);
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(options, lookupCommand, schema, async, maxRetryTimes, retryBackoff, cache);
    }

    @Override
    public String asSummaryString() {
        return "Redis-V2 Table Source";
    }
}
