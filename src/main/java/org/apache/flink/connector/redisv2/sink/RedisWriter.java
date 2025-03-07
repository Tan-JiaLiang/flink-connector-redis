package org.apache.flink.connector.redisv2.sink;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.redisv2.command.write.WriteCommandExec;
import org.apache.flink.connector.redisv2.options.RedisWriteOptions;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RedisWriter<IN> implements SinkWriter<IN> {

    private final WriteCommandExec<IN> command;
    private final ScheduledExecutorService executor;
    private final List<RedisFuture<?>> pendingFutures;
    private final RedisWriterMetrics metrics;
    private final Histogram flushLatenciesHistogram;
    private final long bufferFlushMaxRows;
    private final Duration timeout;

    private volatile int lastFlushRows = 0;
    private volatile long lastFlushTime = 0L;
    private volatile long lastFlushInternal = 0L;
    private volatile boolean closed = false;
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public RedisWriter(WriteCommandExec<IN> command, RedisWriteOptions writeOptions, RedisWriterMetrics metrics) {
        this.command = command;
        this.pendingFutures = new ArrayList<>((int) writeOptions.getBufferFlushMaxRows());
        this.bufferFlushMaxRows = writeOptions.getBufferFlushMaxRows();
        this.timeout = writeOptions.getTimeout();
        this.metrics = metrics;
        this.executor =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("redis-upsert-sink-flusher"));
        this.executor.scheduleWithFixedDelay(
                () -> {
                    if (closed) {
                        return;
                    }
                    try {
                        flush(false);
                    } catch (Exception e) {
                        // fail the sink and skip the rest of the items
                        // if the failure handler decides to throw an exception
                        failureThrowable.compareAndSet(null, e);
                    }
                },
                writeOptions.getBufferFlushIntervalMillis(),
                writeOptions.getBufferFlushIntervalMillis(),
                TimeUnit.MILLISECONDS
        );

        // metrics
        this.flushLatenciesHistogram = new DescriptiveStatisticsHistogram(100);
        metrics.flushRowsGauge(() -> lastFlushRows);
        metrics.flushLatenciesNanosHistogram(flushLatenciesHistogram);
        metrics.flushInternalGauge(() -> lastFlushInternal);
    }

    @Override
    public synchronized void write(IN element, Context context) throws IOException {
        checkErrorAndRethrow();
        List<RedisFuture<?>> futures = command.write(element);
        pendingFutures.addAll(futures);

        // 满N条执行flush操作
        if (pendingFutures.size() >= bufferFlushMaxRows) {
            flush(false);
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        long startTime = System.currentTimeMillis();
        if (lastFlushTime != 0) {
            lastFlushInternal = startTime - lastFlushTime;
        }
        lastFlushTime = startTime;

        checkErrorAndRethrow();
        if (pendingFutures.isEmpty()) {
            return;
        }
        command.flush();
        boolean timeout =
                LettuceFutures.awaitAll(
                        this.timeout.getSeconds(),
                        TimeUnit.SECONDS,
                        pendingFutures.toArray(new RedisFuture[0])
                );
        lastFlushRows = pendingFutures.size();
        pendingFutures.clear();
        if (!timeout) {
            throw new RuntimeException("redis batch write timeout");
        }

        long endTime = System.currentTimeMillis();
        flushLatenciesHistogram.update(endTime - startTime);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (command != null) {
            command.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        pendingFutures.clear();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in RedisSink.", cause);
        }
    }

    @VisibleForTesting
    public RedisWriterMetrics getMetrics() {
        return metrics;
    }
}
