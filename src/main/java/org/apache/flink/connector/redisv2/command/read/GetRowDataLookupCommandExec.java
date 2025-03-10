package org.apache.flink.connector.redisv2.command.read;

import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class GetRowDataLookupCommandExec extends AbstractRowDataLookupCommandExec {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GetRowDataLookupCommandExec.class);

    private final RedisRowDataConverter.RowDataToStringConverter keyExternalConverter;
    private final RedisRowDataConverter.StringToInternalConverter keyInternalConverter;
    private final RedisRowDataConverter.StringToInternalConverter valueInternalConverter;
    private final int keyIndex;
    private final int valueIndex;
    private final int fieldLength;

    public GetRowDataLookupCommandExec(RedisOptions options, ResolvedSchema schema) {
        super(options);

        // validate
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length != 1) {
            throw new IllegalArgumentException("the GET command must declare the primary key as the lookup key.");
        }

        this.keyIndex = getKeyIndex(schema);
        this.keyExternalConverter = toExternalConverter(schema, keyIndex);
        this.keyInternalConverter = toInternalConverter(schema, keyIndex);

        this.valueIndex = getValueIndex(schema);
        this.valueInternalConverter = toInternalConverter(schema, valueIndex);
        this.fieldLength = schema.getColumnCount();
    }

    @Override
    protected CompletableFuture<Collection<RowData>> lookup(RedisClient client, RowData input) throws IOException {
        CompletableFuture<Collection<RowData>> completableFuture = new CompletableFuture<>();
        String key = keyExternalConverter.convert(input);
        client.get(key)
                .whenCompleteAsync(
                        (result, throwable) -> {
                            if (throwable != null) {
                                LOG.error("lookup error ", throwable);
                                completableFuture.completeExceptionally(throwable);
                            } else {
                                if (result == null) {
                                    completableFuture.complete(null);
                                } else {
                                    GenericRowData output = new GenericRowData(fieldLength);
                                    output.setField(keyIndex, keyInternalConverter.convert(key));
                                    output.setField(valueIndex, valueInternalConverter.convert(result));
                                    completableFuture.complete(Collections.singletonList(output));
                                }
                            }
                        });
        return completableFuture;
    }
}
