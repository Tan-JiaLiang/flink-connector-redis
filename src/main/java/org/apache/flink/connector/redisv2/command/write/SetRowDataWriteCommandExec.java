package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SetRowDataWriteCommandExec extends AbstractRowDataWriteCommandExec {

    private static final long serialVersionUID = 1L;

    private final boolean nx;
    private final RedisRowDataConverter.RowDataToStringConverter keyInternalConverter;
    private final RedisRowDataConverter.RowDataToStringConverter valueInternalConverter;

    public SetRowDataWriteCommandExec(RedisOptions options,
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
        if (values.size() != 2 || primaryKeyIndexes.length != 1) {
            throw new IllegalArgumentException(
                    "the SET command only support two physical columns, " +
                            "and the column for the redis key must declare as the primary key.");
        }

        int keyIndex = getKeyIndex(schema);
        this.keyInternalConverter = toExternalConverter(schema, keyIndex);

        int valueIndex = getValueIndex(schema);
        this.valueInternalConverter = toExternalConverter(schema, valueIndex);
    }

    @Override
    protected List<RedisFuture<?>> put(RedisClient client, RowData input, @Nullable Long expire) {
        SetArgs args = new SetArgs();
        if (nx) {
            args = args.nx();
        }
        if (expire != null) {
            args = args.ex(expire);
        }

        String key = keyInternalConverter.convert(input);
        String value = valueInternalConverter.convert(input);
        return Collections.singletonList(client.set(key, value, args));
    }

    @Override
    protected List<RedisFuture<?>> del(RedisClient client, RowData input) {
        String key = keyInternalConverter.convert(input);
        return Collections.singletonList(client.del(key));
    }
}
