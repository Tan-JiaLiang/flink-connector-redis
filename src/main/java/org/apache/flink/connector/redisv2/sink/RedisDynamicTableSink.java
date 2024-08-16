package org.apache.flink.connector.redisv2.sink;

import org.apache.flink.connector.redisv2.command.RedisCommand;
import org.apache.flink.connector.redisv2.command.RedisRowDataCommandFactory;
import org.apache.flink.connector.redisv2.command.write.WriteCommandExec;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.options.RedisWriteOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final RedisCommand command;
    private final RedisOptions options;
    private final RedisWriteOptions writeOptions;
    private final ResolvedSchema schema;
    @Nullable private final String putScriptPath;
    @Nullable private final String delScriptPath;

    private List<String> metadataKeys;

    public RedisDynamicTableSink(RedisCommand command,
                                 RedisOptions options,
                                 RedisWriteOptions writeOptions,
                                 ResolvedSchema schema,
                                 @Nullable String putScriptPath,
                                 @Nullable String delScriptPath) {
        this.command = command;
        this.options = options;
        this.writeOptions = writeOptions;
        this.schema = schema;
        this.metadataKeys = Collections.emptyList();
        this.putScriptPath = putScriptPath;
        this.delScriptPath = delScriptPath;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        WriteCommandExec<RowData> writeCommandExec =
                RedisRowDataCommandFactory.createWriteCommandExec(
                        command,
                        options,
                        schema,
                        metadataKeys,
                        putScriptPath,
                        delScriptPath
                );
        return SinkV2Provider.of(new RedisSink<>(writeCommandExec, writeOptions), writeOptions.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(command, options, writeOptions, schema, putScriptPath, delScriptPath);
    }

    @Override
    public String asSummaryString() {
        return "Redis-V2 Table Sink";
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return WritableMetadata.list();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }
}
