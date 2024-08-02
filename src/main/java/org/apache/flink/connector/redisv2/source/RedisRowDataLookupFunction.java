package org.apache.flink.connector.redisv2.source;

import org.apache.flink.connector.redisv2.command.read.LookupCommandExec;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import java.io.IOException;
import java.util.Collection;

public class RedisRowDataLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final LookupCommandExec<RowData> command;

    public RedisRowDataLookupFunction(LookupCommandExec<RowData> command) {
        this.command = command;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.command.open();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        try {
            return command.lookup(keyRow).get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (command != null) {
            command.close();
        }
    }
}
