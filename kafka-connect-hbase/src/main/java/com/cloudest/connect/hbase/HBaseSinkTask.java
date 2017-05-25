package com.cloudest.connect.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HBaseSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(HBaseSinkTask.class);

    private HBaseClient hBaseClient;
    private HBaseSinkConfig config;

    @Override
    public String version() {
        return HBaseSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new HBaseSinkConfig(props);

        try {
            hBaseClient = new HBaseClient(config.getZookeeperQuorum());
        } catch (IOException e) {
            throw new ConnectException("failed to connect to zookeeper quorum", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record: records) {
            String tableName = tableName(record.topic());
            try {
                Struct mutations = extractData(record);
                if (mutations != null){
                    hBaseClient.write(tableName, toMutations(mutations));
                } else {  // (mutations == null) <=> Delete
                    hBaseClient.write(tableName, new Delete(rowkey(mutations)));
                }
            } catch (IOException e) {
                throw new ConnectException("Failed to write record to hbase", e);
            }
        }
    }

    private String tableName(String topic) {
        if (config.getTableName().isEmpty()) {
            return topic;
        }else {
            return config.getTableName();
        }
    }

    private Struct extractData(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        String dataField = config.getDataField();
        if (! (value instanceof Struct) ) {
            throw new DataException("SinkRecord's value is not of struct type.");
        }
        if (dataField.length() > 0) {
            value = ((Struct) value).getStruct(dataField);
            if (! (value instanceof Struct) ) {
                throw new DataException("Value's data field is not of struct type.");
            }
        }

        return (Struct) value;
    }

    private static byte[] getFieldData(Struct data, String field) {
        if (data.get(field) == null) {
            return null;
        }
        Schema schema = data.schema();
        switch(schema.field(field).schema().type()) {
        case BOOLEAN:
            return Bytes.toBytes(data.getBoolean(field));
        case STRING:
            /*
            if ("id".equals(field)) {
                try {
                    UUID uuid = UUID.fromString(data.getString(field));
                    byte[] uuidBytes = new byte[Bytes.SIZEOF_LONG * 2];
                    Bytes.putLong(uuidBytes, 0, uuid.getMostSignificantBits());
                    Bytes.putLong(uuidBytes, Bytes.SIZEOF_LONG, uuid.getLeastSignificantBits());
                    return uuidBytes;
                } catch(IllegalArgumentException e) {
                }
            }
            */
            return Bytes.toBytes(data.getString(field));
        case BYTES:
            return data.getBytes(field);
        case INT8:
            return Bytes.toBytes(data.getInt8(field));
        case INT16:
            return Bytes.toBytes(data.getInt16(field));
        case INT32:
            return Bytes.toBytes(data.getInt32(field));
        case INT64:
            return Bytes.toBytes(data.getInt64(field));
        case FLOAT32:
            return Bytes.toBytes(data.getFloat32(field));
        case FLOAT64:
            return Bytes.toBytes(data.getFloat64(field));
        default:
            return null;    
        }
    }

    private byte[] rowkey(Struct data) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (String col: config.getRowkeyColumns()) {
            byte[] val = getFieldData(data, col);
            if (baos.size()>0) {
                    baos.write(Bytes.toBytes(config.getRowkeyDelimiter()));
            }
            if (val != null) {
                baos.write(val);
            }
        }
        return baos.toByteArray();
    }

    private List<Mutation> toMutations(Struct data) throws IOException {
        byte[] rowKey = rowkey(data);
        Put put = new Put(rowKey);
        Delete delete = new Delete(rowKey);

        byte[] cf = Bytes.toBytes(config.getColumnFamily());

        for (Field field: data.schema().fields()) {
            String fieldName = field.name();
            byte[] fieldValue = getFieldData(data, fieldName);
            if (fieldValue != null) {
                put.addColumn(cf,
                              Bytes.toBytes(fieldName),
                              fieldValue);
            }else {
                delete.addColumn(cf, Bytes.toBytes(fieldName));
            }
        }

        List<Mutation> mutations = new ArrayList<>();

        if (!put.isEmpty()) {
            mutations.add(put);
        }
        if (!delete.isEmpty()) {
            mutations.add(delete);
        }
        return mutations;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            hBaseClient.flush();
        } catch (IOException e) {
            throw new ConnectException("failed to flush hbase puts", e);
        }
    }

    @Override
    public void stop() {
        try {
            hBaseClient.close();
        } catch (IOException e) {
            throw new ConnectException("failed to close hbase", e);
        }
    }

}
