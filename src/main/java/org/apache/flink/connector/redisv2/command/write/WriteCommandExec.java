package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface WriteCommandExec<T> extends Serializable {

    void open() throws IOException;

    List<RedisFuture<?>> write(T value) throws IOException;

    void flush() throws IOException;

    void close() throws IOException;
}
