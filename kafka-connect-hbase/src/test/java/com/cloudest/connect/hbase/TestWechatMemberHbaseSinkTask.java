package com.cloudest.connect.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class TestWechatMemberHbaseSinkTask {

    private final String hbaseTable = "test"; // using this interchangeably with kafka topic name.
    private final String hbaseSubTable = "test_sub";

    private final Map<String, String> props = new HashMap<>();
    private Configuration configuration;
    private String after;

    @Before
    public void setUp() throws Exception {
        HbaseTestUtil.startMiniCluster();
        HbaseTestUtil.createTable(hbaseTable,
                                  WechatMemberHBaseSinkTask.BASE_CF,
                                  WechatMemberHBaseSinkTask.DEVICE_TYPE_CF,
                                  WechatMemberHBaseSinkTask.HOT_IND_CF,
                                  WechatMemberHBaseSinkTask.MEDIAS_CF);

        HbaseTestUtil.createTable(hbaseSubTable,
                                  WechatMemberHBaseSinkTask.BASE_CF,
                                  WechatMemberHBaseSinkTask.PUSH_CF,
                                  WechatMemberHBaseSinkTask.MEDIAS_CF);

        configuration = HbaseTestUtil.getUtility().getConfiguration();

        //configure defaults for Sink task.
        props.put(WechatMemberHBaseSinkConfig.HBASE_TABLE_NAME_CONFIG, hbaseTable);
        props.put(WechatMemberHBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG, "localhost:" + HbaseTestUtil.getUtility().getZkCluster().getClientPort());
        
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("wechatMember.json")));
        String line;
        while((line = reader.readLine())!=null) {
            sb.append(line);
        }
        after = sb.toString();
    }

    @Test
    public void testConnect() throws Exception {
        writeAndValidate();
    }

    private void writeAndValidate() throws IOException {
        WechatMemberHBaseSinkTask task = new WechatMemberHBaseSinkTask();
        task.start(props);


        final Schema keySchema = SchemaBuilder.struct().name("key").version(1)
                .field("_id", Schema.STRING_SCHEMA).build();

        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
                .field("after", Schema.OPTIONAL_STRING_SCHEMA)
                .field("patch", Schema.OPTIONAL_STRING_SCHEMA).build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();

        final Struct key = new Struct(keySchema).put("_id", "oc7fLvpYSuzf9MtzyA6PiQG9a3sg");
        final Struct value = new Struct(valueSchema).put("after", after)
                .put("patch", null);
        

        SinkRecord sinkRecord = new SinkRecord("test2", 0, keySchema, key, valueSchema, value, 0);
        sinkRecords.add(sinkRecord);

        task.put(sinkRecords);
        task.stop();

        // read from hbase.
        TableName table = TableName.valueOf(hbaseTable);
        TableName subTable = TableName.valueOf(hbaseSubTable);

        Scan scan = new Scan();
        try (Connection conn = ConnectionFactory.createConnection(configuration);
             Table hTable = conn.getTable(table);
             Table hSubTable = conn.getTable(subTable); ) {

            Get getMain = new Get(Bytes.toBytes("oc7fLvpYSuzf9MtzyA6PiQG9a3sg"));
            Result main = hTable.get(getMain);

            Get getFulishe = new Get(Bytes.toBytes("oc7fLvpYSuzf9MtzyA6PiQG9a3sg/fulishe"));
            Result fulishe = hSubTable.get(getFulishe);

            Get getShenzhen = new Get(Bytes.toBytes("oc7fLvpYSuzf9MtzyA6PiQG9a3sg/shenzhen"));
            Result shenzhen = hSubTable.get(getShenzhen);

            
            String lang = Bytes.toString(main.getValue(Bytes.toBytes(WechatMemberHBaseSinkTask.BASE_CF), Bytes.toBytes("lang")));
            int price = Bytes.toInt(main.getValue(Bytes.toBytes(WechatMemberHBaseSinkTask.BASE_CF), Bytes.toBytes("price")));
            
            int shenzhenPush = Bytes.toInt(shenzhen.getValue(Bytes.toBytes(WechatMemberHBaseSinkTask.PUSH_CF), Bytes.toBytes("2017-02"))); 
            
            Assert.assertEquals("zh_CN", lang);
            Assert.assertEquals(4, price);

            Assert.assertEquals(1, shenzhenPush);

        }
    }

    @After
    public void tearDown() throws Exception {
        HbaseTestUtil.stopMiniCluster();
    }
}
