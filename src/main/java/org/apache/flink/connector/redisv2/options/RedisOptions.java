package org.apache.flink.connector.redisv2.options;

import org.apache.flink.connector.redisv2.table.RedisMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

public class RedisOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RedisMode mode;
    private final String nodes;
    private final Duration timeout;
    private final Integer ioThreadPoolSize;
    private final Integer computationThreadPoolSize;
    private final String password;
    private final Boolean autoFlushCommands;

    private RedisOptions(
            RedisMode mode,
            String nodes,
            Duration timeout,
            Integer ioThreadPoolSize,
            Integer computationThreadPoolSize,
            String password,
            Boolean autoFlushCommands) {
        this.mode = Preconditions.checkNotNull(mode, "mode can not be empty");
        this.timeout = Preconditions.checkNotNull(timeout, "timeout cannot be empty");
        this.nodes = Preconditions.checkNotNull(nodes, "nodes can not be empty");
        this.ioThreadPoolSize = Preconditions.checkNotNull(ioThreadPoolSize, "io.pool.size can not be empty");
        this.computationThreadPoolSize = Preconditions.checkNotNull(computationThreadPoolSize, "computation.pool.size can not be empty");
        this.password = password;
        this.autoFlushCommands = Boolean.TRUE.equals(autoFlushCommands);
    }

    public RedisMode getMode() {
        return mode;
    }

    public String getNodes() {
        return nodes;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public Integer getIoThreadPoolSize() {
        return ioThreadPoolSize;
    }

    public Integer getComputationThreadPoolSize() {
        return computationThreadPoolSize;
    }

    public String getPassword() {
        return password;
    }

    public Boolean isAutoFlushCommands() {
        return autoFlushCommands;
    }

    @Override
    public String toString() {
        return "RedisOptions{" +
                "mode=" + mode +
                ", nodes='" + nodes + '\'' +
                ", timeout=" + timeout +
                ", ioThreadPoolSize=" + ioThreadPoolSize +
                ", computationThreadPoolSize=" + computationThreadPoolSize +
                ", password='" + password + '\'' +
                ", autoFlushCommands=" + autoFlushCommands +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        RedisOptions that = (RedisOptions) object;
        return Objects.equals(ioThreadPoolSize, that.ioThreadPoolSize)
                && Objects.equals(computationThreadPoolSize, that.computationThreadPoolSize)
                && autoFlushCommands == that.autoFlushCommands
                && mode == that.mode
                && Objects.equals(nodes, that.nodes)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, nodes, timeout, ioThreadPoolSize, computationThreadPoolSize, password, autoFlushCommands);
    }

    public static RedisOptions.Builder builder() {
        return new RedisOptions.Builder();
    }

    public static class Builder {
        private RedisMode mode;
        private String nodes;
        private Duration timeout;
        private Integer ioThreadPoolSize;
        private Integer computationThreadPoolSize;
        private String password;
        private Boolean autoFlushCommands = true;

        public Builder setMode(RedisMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder setNodes(String nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setIoThreadPoolSize(Integer ioThreadPoolSize) {
            this.ioThreadPoolSize = ioThreadPoolSize;
            return this;
        }

        public Builder setComputationThreadPoolSize(Integer computationThreadPoolSize) {
            this.computationThreadPoolSize = computationThreadPoolSize;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setAutoFlushCommands(Boolean autoFlushCommands) {
            this.autoFlushCommands = autoFlushCommands;
            return this;
        }

        public RedisOptions build() {
            return new RedisOptions(
                    mode,
                    nodes,
                    timeout,
                    ioThreadPoolSize,
                    computationThreadPoolSize,
                    password,
                    autoFlushCommands);
        }
    }
}
