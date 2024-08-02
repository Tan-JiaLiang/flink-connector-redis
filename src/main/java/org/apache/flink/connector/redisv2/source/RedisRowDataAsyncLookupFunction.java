package org.apache.flink.connector.redisv2.source;

import org.apache.flink.connector.redisv2.command.read.LookupCommandExec;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class RedisRowDataAsyncLookupFunction extends AsyncLookupFunction {

    private static final long serialVersionUID = 1L;

    private final LookupCommandExec<RowData> command;

    public RedisRowDataAsyncLookupFunction(LookupCommandExec<RowData> command) {
        this.command = command;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.command.open();
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        try {
            return command.lookup(keyRow);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (command != null) {
            command.close();
        }
    }
}
