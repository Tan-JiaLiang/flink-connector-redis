package org.apache.flink.connector.redisv2.command;

import org.apache.flink.connector.redisv2.command.read.GetRowDataLookupCommandExec;
import org.apache.flink.connector.redisv2.command.read.HGetRowDataLookupCommandExec;
import org.apache.flink.connector.redisv2.command.read.LookupCommandExec;
import org.apache.flink.connector.redisv2.command.write.*;
import org.apache.flink.connector.redisv2.options.RedisOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;
import java.util.List;

public class RedisRowDataCommandFactory {

    public static WriteCommandExec<RowData> createWriteCommandExec(RedisCommand command,
                                                                   RedisOptions options,
                                                                   ResolvedSchema schema,
                                                                   List<String> metadataKeys,
                                                                   @Nullable String putScriptPath,
                                                                   @Nullable String delScriptPath) {
        switch (command) {
            case SET:
                return new SetRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema,
                        false);
            case SETNX:
                return new SetRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema,
                        true);
            case HSET:
                return new HSetRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema,
                        false);
            case HSETNX:
                return new HSetRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema,
                        true);
            case INCRBY:
                return new IncrByRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema);
            case LUA:
                return new LuaRowDataWriteCommandExec(
                        options,
                        schema.toPhysicalRowDataType(),
                        metadataKeys,
                        schema,
                        putScriptPath,
                        delScriptPath);
            default:
                throw new IllegalArgumentException("Unsupported command: " + command);
        }
    }

    public static LookupCommandExec<RowData> createLookupCommandExec(RedisCommand command,
                                                                     RedisOptions options,
                                                                     ResolvedSchema schema) {
        switch (command) {
            case GET:
                return new GetRowDataLookupCommandExec(options, schema);
            case HGET:
                return new HGetRowDataLookupCommandExec(options, schema);
            default:
                throw new IllegalArgumentException("Unsupported command: " + command);
        }
    }
}
