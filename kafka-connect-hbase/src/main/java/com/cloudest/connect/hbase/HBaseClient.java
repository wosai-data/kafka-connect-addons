package com.cloudest.connect.hbase;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HBaseClient {

    private String zkQuorum;
    private Connection connection;
    private Map<TableName, BufferedMutator> bufferByTable = new HashMap<TableName, BufferedMutator>();

    public HBaseClient(String zkQuorum) throws IOException {
        this.zkQuorum = zkQuorum;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    public void write(final String tableName, final List<Put> puts) throws IOException {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(puts);
        final TableName table = TableName.valueOf(tableName);
        write(table, puts);
    }

    public void write(final String tableName, final Put put) throws IOException {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(put);
        final TableName table = TableName.valueOf(tableName);
        write(table, put);
    }

    public void flush() throws IOException {
        for (BufferedMutator bm: bufferByTable.values()) {
            bm.flush();
        }
    }

    public void close() throws IOException {
        for (BufferedMutator bm: bufferByTable.values()) {
            bm.close();
        }
    }

    private BufferedMutator getBuffer(TableName tableName) throws IOException {
        BufferedMutator buffer = bufferByTable.get(tableName);
        if (buffer == null) {
            buffer = connection.getBufferedMutator(tableName);
            bufferByTable.put(tableName, buffer);
        }
        return buffer;
    }

    private void write(final TableName table, final List<Put> puts) throws IOException {

        BufferedMutator bm = getBuffer(table);
        bm.mutate(puts);
    }

    private void write(final TableName table, final Put put) throws IOException {

        BufferedMutator bm = getBuffer(table);
        bm.mutate(put);
    }
}
