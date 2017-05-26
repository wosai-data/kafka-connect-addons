package com.cloudest.connect.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class TestHbaseSinkTask {

    private final String hbaseTable = "test"; // using this interchangeably with kafka topic name.
    private final String columnFamily = "d";
    private final Map<String, String> props = new HashMap<>();
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        HbaseTestUtil.startMiniCluster();
        HbaseTestUtil.createTable(hbaseTable, columnFamily);

        configuration = HbaseTestUtil.getUtility().getConfiguration();

        //configure defaults for Sink task.
        props.put(HBaseSinkConfig.DATA_FIELD_CONFIG, "after");
        props.put(HBaseSinkConfig.HBASE_TABLE_NAME_CONFIG, hbaseTable);
        props.put(HBaseSinkConfig.HBASE_ROWKEY_COLUMNS_CONFIG, "id");
        props.put(HBaseSinkConfig.HBASE_ROWKEY_DELIMITER_CONFIG, "|");
        props.put(HBaseSinkConfig.HBASE_COLUMN_FAMILY_CONFIG, columnFamily);
        // props.put("topics", hbaseTable);
        props.put(HBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG, "localhost:" + HbaseTestUtil.getUtility().getZkCluster().getClientPort());
    }

    @Test
    public void testConnect() throws Exception {
        writeAndValidate();
    }

    private void writeAndValidate() throws IOException {
        HBaseSinkTask task = new HBaseSinkTask();
        task.start(props);

        final Schema dataSchema = SchemaBuilder.struct().name("data").version(1).optional()
          .field("url", Schema.STRING_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("zipcode", Schema.INT32_SCHEMA)
          .field("status", Schema.INT32_SCHEMA)
          .build();

        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
                .field("before", dataSchema)
                .field("after", dataSchema).build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        int noOfRecords = 10;
        for (int i = 1; i <= noOfRecords; i++) {
            final Struct data = new Struct(dataSchema)
              .put("url", "google.com")
              .put("name", null)
              .put("id", i)
              .put("zipcode", 95050 + i)
              .put("status", 400 + i);
            final Struct record = new Struct(valueSchema).put("after", data);

            SinkRecord sinkRecord = new SinkRecord("test2", 0, null, null, dataSchema, record, i);
            sinkRecords.add(sinkRecord);
        }

        final Struct data = new Struct(dataSchema)
              .put("url", "google.com")
              .put("name", null)
              .put("id", noOfRecords)
              .put("zipcode", 95050 + noOfRecords)
              .put("status", 400 + noOfRecords);
        final Struct record = new Struct(valueSchema).put("after", null).put("before",data);
        SinkRecord sinkRecord = new SinkRecord("test2", 0, null, null, dataSchema, record, noOfRecords);
        sinkRecords.add(sinkRecord);

        task.put(sinkRecords);
        task.stop();

        // read from hbase.
        TableName table = TableName.valueOf(hbaseTable);
        Scan scan = new Scan();
        try (Table hTable = ConnectionFactory.createConnection(configuration).getTable(table);
             ResultScanner results = hTable.getScanner(scan); ) {

            int count = 0;
            for (Result result : results) {
                int rowId = Bytes.toInt(result.getRow());
                String url = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("url")));
                String name = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("name")));
                Assert.assertEquals(count + 1, rowId);
                Assert.assertEquals("google.com", url);
                Assert.assertEquals(null, name);
                count++;
            }
            Assert.assertEquals(noOfRecords-1, count);
        }
    }

    @After
    public void tearDown() throws Exception {
        HbaseTestUtil.stopMiniCluster();
    }
}
