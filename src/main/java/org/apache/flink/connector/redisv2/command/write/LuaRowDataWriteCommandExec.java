package org.apache.flink.connector.redisv2.command.write;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.apache.flink.connector.redisv2.client.RedisClient;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.connector.redisv2.serde.RedisRowDataConverter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LuaRowDataWriteCommandExec extends AbstractRowDataWriteCommandExec {

    private static final long serialVersionUID = 1L;

    private final String putScriptPath;
    private final String delScriptPath;

    private final List<RedisRowDataConverter.RowDataToStringConverter> keyConverters;
    private final List<RedisRowDataConverter.RowDataToStringConverter> valueConverters;

    private transient String putScript;
    private transient String delScript;

    public LuaRowDataWriteCommandExec(RedisOptions options,
                                      DataType physicalRowDataType,
                                      List<String> metadataKeys,
                                      ResolvedSchema schema,
                                      String putScriptPath,
                                      String delScriptPath) {
        super(options, physicalRowDataType, metadataKeys);
        Preconditions.checkNotNull(putScriptPath, "putScript can not be empty.");
        Preconditions.checkNotNull(delScriptPath, "delScript can not be empty.");

        this.putScriptPath = putScriptPath;
        this.delScriptPath = delScriptPath;

        this.keyConverters = new ArrayList<>();
        List<String> primaryKey =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList());

        this.valueConverters = new ArrayList<>();
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (primaryKey.contains(column.getName())) {
                this.keyConverters.add(toExternalConverter(schema, i));
            } else {
                this.valueConverters.add(toExternalConverter(schema, i));
            }
        }
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.putScript = getLuaScript(putScriptPath);
        this.delScript = getLuaScript(delScriptPath);
    }

    @Override
    protected List<RedisFuture<?>> put(RedisClient client, RowData input, @Nullable Long expire) {
        String[] keys =
                keyConverters.stream()
                        .map(converter -> converter.convert(input))
                        .collect(Collectors.toList())
                        .toArray(new String[]{});
        String[] values =
                valueConverters.stream()
                        .map(converter -> converter.convert(input))
                        .collect(Collectors.toList())
                        .toArray(new String[]{});
        return Collections.singletonList(client.eval(putScript, ScriptOutputType.STATUS, keys, values));
    }

    @Override
    protected List<RedisFuture<?>> del(RedisClient client, RowData input) {
        String[] keys =
                valueConverters.stream()
                        .map(converter -> converter.convert(input))
                        .collect(Collectors.toList())
                        .toArray(new String[]{});
        String[] values =
                valueConverters.stream()
                        .map(converter -> converter.convert(input))
                        .collect(Collectors.toList())
                        .toArray(new String[]{});
        return Collections.singletonList(client.eval(delScript, ScriptOutputType.STATUS, keys, values));
    }

    private String getLuaScript(String path) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        URL putURL = classloader.getResource(path);
        if (putURL == null) {
            throw new IllegalArgumentException(path + " is not exists in classpath.");
        }
        try {
            File put = new File(putURL.toURI());
            return FileUtils.readFileUtf8(put);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
