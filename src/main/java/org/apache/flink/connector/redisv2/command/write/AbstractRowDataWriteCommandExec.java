package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.client.RedisClientCreator;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.connector.redisv2.sink.WritableMetadata;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.redisv2.serde.RedisRowDataConverter.createNotNullExternalConverter;
import static org.apache.flink.connector.redisv2.serde.RedisRowDataConverter.createNullableInternalConverter;

public abstract class AbstractRowDataWriteCommandExec implements WriteCommandExec<RowData> {

    private final RedisOptions options;
    private final WritableMetadata.ExpireMetadata expireMetadata;

    private transient RedisClient client;

    public AbstractRowDataWriteCommandExec(RedisOptions options,
                                           DataType physicalDataType,
                                           List<String> metadataKeys) {
        this.options = options;
        this.expireMetadata = new WritableMetadata.ExpireMetadata(metadataKeys, physicalDataType);
    }

    @Override
    public void open() throws IOException {
        this.client = RedisClientCreator.createClient(options);
    }

    @Override
    public List<RedisFuture<?>> write(RowData input) throws IOException {
        Long expire = expireMetadata.read(input);

        RowKind kind = input.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            return put(client, input, expire);
        } else {
            return del(client, input);
        }
    }

    @Override
    public void flush() throws IOException {
        client.flush();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    protected abstract List<RedisFuture<?>> put(RedisClient client, RowData input, @Nullable Long expire);

    protected abstract List<RedisFuture<?>> del(RedisClient client, RowData input);

    protected int getKeyIndex(ResolvedSchema schema) {
        return schema.getPrimaryKeyIndexes()[0];
    }

    protected int getFieldIndex(ResolvedSchema schema) {
        return schema.getPrimaryKeyIndexes()[1];
    }

    protected RedisRowDataConverter.RowDataToStringConverter toExternalConverter(ResolvedSchema schema, int index) {
        Column column = schema.getColumns().get(index);
        LogicalType logicalType = column.getDataType().getLogicalType();
        String keyName = column.getName();
        return createNotNullExternalConverter(keyName, logicalType, index);
    }

    protected RedisRowDataConverter.StringToInternalConverter toInternalConverter(ResolvedSchema schema, int index) {
        Column column = schema.getColumns().get(index);
        LogicalType logicalType = column.getDataType().getLogicalType();
        return createNullableInternalConverter(logicalType);
    }

    protected int getValueIndex(ResolvedSchema schema) {
        int valueIndex = -1;
        List<Column> columns = schema.getColumns();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            int index = i;
            if (column.isPhysical() && Arrays.stream(primaryKeyIndexes).noneMatch(x -> x == index)) {
                valueIndex = i;
                break;
            }
        }
        if (valueIndex == -1) {
            throw new IllegalArgumentException("the GET command must declare a field as the value.");
        }
        return valueIndex;
    }
}
