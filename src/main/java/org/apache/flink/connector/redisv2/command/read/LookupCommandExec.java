package org.apache.flink.connector.redisv2.command.read;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface LookupCommandExec<T> extends Serializable {

    void open() throws IOException;

    CompletableFuture<Collection<RowData>> lookup(T value) throws IOException;

    void close() throws IOException;
}
