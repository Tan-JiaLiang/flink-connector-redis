package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HSetRowDataWriteCommandExec extends AbstractRowDataWriteCommandExec {

    private static final long serialVersionUID = 1L;

    private final boolean nx;
    private final RedisRowDataConverter.RowDataToStringConverter keyInternalConverter;
    private final RedisRowDataConverter.RowDataToStringConverter fieldInternalConverter;
    private final RedisRowDataConverter.RowDataToStringConverter valueInternalConverter;

    public HSetRowDataWriteCommandExec(RedisOptions options,
                                       DataType physicalDataType,
                                       List<String> metadataKeys,
                                       ResolvedSchema schema,
                                       boolean nx) {
        super(options, physicalDataType, metadataKeys);
        this.nx = nx;

        // validate
        List<Column> columns = schema.getColumns();
        List<Column> values =
                columns.stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        if (values.size() != 3 || primaryKeyIndexes.length != 2) {
            throw new IllegalArgumentException(
                    "the HSET command only support three physical columns, " +
                            "and the column for the redis key and field must declare as the primary key.");
        }

        int keyIndex = getKeyIndex(schema);
        this.keyInternalConverter = toExternalConverter(schema, keyIndex);

        int fieldIndex = getFieldIndex(schema);
        this.fieldInternalConverter = toExternalConverter(schema, fieldIndex);

        int valueIndex = getValueIndex(schema);
        this.valueInternalConverter = toExternalConverter(schema, valueIndex);
    }

    @Override
    protected List<RedisFuture<?>> put(RedisClient client, RowData input, @Nullable Long expire) {
        String key = keyInternalConverter.convert(input);
        String field = fieldInternalConverter.convert(input);
        String value = valueInternalConverter.convert(input);

        RedisFuture<Boolean> hset;
        if (nx) {
            hset = client.hsetnx(key, field, value);
        } else {
            hset = client.hset(key, field, value);
        }

        if (expire != null) {
            RedisFuture<Boolean> expireKey = client.expire(key, expire);
            return Arrays.asList(hset, expireKey);
        }

        return Collections.singletonList(hset);
    }

    @Override
    protected List<RedisFuture<?>> del(RedisClient client, RowData input) {
        String key = keyInternalConverter.convert(input);
        String field = fieldInternalConverter.convert(input);
        return Collections.singletonList(client.hdel(key, field));
    }
}
