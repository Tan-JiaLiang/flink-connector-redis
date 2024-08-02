package org.apache.flink.connector.redisv2.sink;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

public class RedisWriterMetrics extends ProxyMetricGroup<MetricGroup> {

    public static final String FLUSH_LATENCIES_NANOS = "flushLatenciesNanos";
    public static final String FLUSH_ROWS = "flushRows";
    public static final String FLUSH_INTERNAL = "flushInternal";

    public RedisWriterMetrics(MetricGroup metricGroup) {
        super(metricGroup.addGroup("RedisWriter"));
    }

    public void flushLatenciesNanosHistogram(Histogram flushLatenciesNanos) {
        histogram(FLUSH_LATENCIES_NANOS, flushLatenciesNanos);
    }

    public void flushRowsGauge(Gauge<Integer> flushRows) {
        gauge(FLUSH_ROWS, flushRows);
    }

    public void flushInternalGauge(Gauge<Long> flushInternal) {
        gauge(FLUSH_INTERNAL, flushInternal);
    }
}
