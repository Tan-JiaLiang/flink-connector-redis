package org.apache.flink.connector.redisv2.command.read;

import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.client.RedisClientCreator;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.connector.redisv2.serde.RedisRowDataConverter.createNotNullExternalConverter;
import static org.apache.flink.connector.redisv2.serde.RedisRowDataConverter.createNullableInternalConverter;

public abstract class AbstractRowDataLookupCommandExec implements LookupCommandExec<RowData> {

    private final RedisOptions options;

    private transient RedisClient client;

    public AbstractRowDataLookupCommandExec(RedisOptions options) {
        this.options = options;
    }

    @Override
    public void open() throws IOException {
        this.client = RedisClientCreator.createClient(options);
    }

    @Override
    public CompletableFuture<Collection<RowData>> lookup(RowData input) throws IOException {
        return lookup(client, input);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    protected abstract CompletableFuture<Collection<RowData>> lookup(RedisClient client, RowData input) throws IOException;

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
