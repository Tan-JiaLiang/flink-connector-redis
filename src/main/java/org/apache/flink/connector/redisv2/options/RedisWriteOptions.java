package org.apache.flink.connector.redisv2.options;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

public class RedisWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long bufferFlushMaxRows;
    private final long bufferFlushIntervalMillis;
    private final Duration timeout;
    private final Integer parallelism;

    private RedisWriteOptions(
            long bufferFlushMaxMutations,
            long bufferFlushIntervalMillis,
            Duration timeout,
            Integer parallelism) {
        this.bufferFlushMaxRows = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
        this.timeout = timeout;
        this.parallelism = parallelism;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    @Override
    public String toString() {
        return "RedisWriteOptions{" +
                "bufferFlushMaxRows=" + bufferFlushMaxRows +
                ", bufferFlushIntervalMillis=" + bufferFlushIntervalMillis +
                ", timeout=" + timeout +
                ", parallelism=" + parallelism +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        RedisWriteOptions that = (RedisWriteOptions) object;
        return bufferFlushMaxRows == that.bufferFlushMaxRows
                && bufferFlushIntervalMillis == that.bufferFlushIntervalMillis
                && timeout == that.timeout
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferFlushMaxRows, bufferFlushIntervalMillis, timeout, parallelism);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long bufferFlushMaxRows = 0;
        private long bufferFlushIntervalMillis = 0;
        private Integer parallelism;
        private Duration timeout;

        public Builder setBufferFlushMaxRows(long bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        public Builder setBufferFlushIntervalMillis(long bufferFlushIntervalMillis) {
            this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
            return this;
        }

        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public RedisWriteOptions build() {
            return new RedisWriteOptions(bufferFlushMaxRows, bufferFlushIntervalMillis, timeout, parallelism);
        }
    }
}
