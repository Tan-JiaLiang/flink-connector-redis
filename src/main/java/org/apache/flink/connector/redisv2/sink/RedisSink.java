package org.apache.flink.connector.redisv2.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.redisv2.command.write.WriteCommandExec;
import org.apache.flink.connector.redisv2.options.RedisWriteOptions;

import java.io.IOException;

public class RedisSink<IN> implements Sink<IN> {

    private final WriteCommandExec<IN> command;
    private final RedisWriteOptions writeOptions;

    public RedisSink(WriteCommandExec<IN> command, RedisWriteOptions writeOptions) {
        this.command = command;
        this.writeOptions = writeOptions;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        this.command.open();
        RedisWriterMetrics metrics = new RedisWriterMetrics(context.metricGroup());
        return new RedisWriter<>(command, writeOptions, metrics);
    }
}
