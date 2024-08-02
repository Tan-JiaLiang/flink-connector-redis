package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

public class IncrByRowDataWriteCommandExec extends AbstractRowDataWriteCommandExec {

    private static final long serialVersionUID = 1L;

    private final RedisRowDataConverter.RowDataToStringConverter keyInternalConverter;
    private final RedisRowDataConverter.RowDataToStringConverter valueInternalConverter;

    public IncrByRowDataWriteCommandExec(RedisOptions options,
                                         DataType physicalDataType,
                                         List<String> metadataKeys,
                                         ResolvedSchema schema) {
        super(options, physicalDataType, metadataKeys);

        // validate
        List<Column> columns = schema.getColumns();
        List<Column> values =
                columns.stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        if (values.size() != 2 || primaryKeyIndexes.length != 1) {
            throw new IllegalArgumentException(
                    "the SET command only support two physical columns, " +
                            "and the column for the redis key must declare as the primary key.");
        }

        int keyIndex = getKeyIndex(schema);
        this.keyInternalConverter = toExternalConverter(schema, keyIndex);

        int valueIndex = getValueIndex(schema);
        checkArgument(columns.get(valueIndex).getDataType().getLogicalType().is(LogicalTypeRoot.BIGINT),
                "the value must be BIGINT when using INCRBY command");
        this.valueInternalConverter = toExternalConverter(schema, valueIndex);
    }

    @Override
    protected List<RedisFuture<?>> put(RedisClient client, RowData input, @Nullable Long expire) {
        String key = keyInternalConverter.convert(input);
        String value = valueInternalConverter.convert(input);
        RedisFuture<Long> incrby = client.incrby(key, Long.parseLong(value));

        if (expire != null) {
            RedisFuture<Boolean> expr = client.expire(key, expire);
            return Arrays.asList(incrby, expr);
        }

        return Collections.singletonList(incrby);
    }

    @Override
    protected List<RedisFuture<?>> del(RedisClient client, RowData input) {
        String key = keyInternalConverter.convert(input);
        String value = valueInternalConverter.convert(input);
        return Collections.singletonList(client.decrby(key, Long.parseLong(value)));
    }
}
