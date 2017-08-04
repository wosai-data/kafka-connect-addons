package com.cloudest.connect.hbase;

import com.cloudest.connect.hbase.transformer.Transformer;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
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
        for (SinkRecord record : records) {
            String tableName = tableName(record.topic());
            try {
                Struct payload = (Struct) record.value();
                if (payload == null) { // handle tombstone event (delete)
                    handleDeleteForBinlog(record, tableName);
                } else {
                    String op = payload.getString("op");
                    if (StringUtils.equals("u", op) || StringUtils.equals("c", op)) {  // handle create and update
                        List<Mutation> mutations = toMutations(extractData(record));
                        if ( mutations != null && mutations.size() > 0 ) {
                            hBaseClient.write(tableName, mutations);
                        }
                    }
                }

            } catch (IOException e) {
                logger.error("Failed to write to hbase.record is {}.", record, e);
                throw new ConnectException("Failed to write record to hbase", e);
            }
        }
    }

    /**
     * binlog  key is a struct with pk
     *
     *
     * {
     "schema": {
         "type": "struct",
         "name": "mysql-server-1.inventory.customers.Key"
         "optional": false,
         "fields": [
         {
         "field": "id",
         "type": "int32",
         "optional": false
         }
         ]
         },
         "payload": {
         "id": 1001
         }
     }
     *
     *
     * @param record
     * @param tableName
     * @throws IOException
     */
    private void handleDeleteForBinlog(SinkRecord record, String tableName) throws IOException {
        byte[] keyBytes = null;
        Schema.Type keySchemaType = null;
        String field_id = "id";
        if (record.keySchema().field(field_id) != null) {
           keySchemaType =record.keySchema().field(field_id).schema().type();
        }
        Struct key = (Struct) record.key();
        if (keySchemaType != null) {
            switch(keySchemaType) {
                case BOOLEAN:
                    keyBytes = Bytes.toBytes(key.getBoolean(field_id));
                    break;
                case STRING:
                    keyBytes =  Bytes.toBytes(key.getString(field_id));
                    break;
                case BYTES:
                    keyBytes =  key.getBytes(field_id);
                    break;
                case INT8:
                    keyBytes =  Bytes.toBytes(key.getInt8(field_id));
                    break;
                case INT16:
                    keyBytes =  Bytes.toBytes(key.getInt16(field_id));
                    break;
                case INT32:
                    keyBytes =  Bytes.toBytes(key.getInt32(field_id));
                    break;
                case INT64:
                    keyBytes =  Bytes.toBytes(key.getInt64(field_id));
                    break;
                case FLOAT32:
                    keyBytes =  Bytes.toBytes(key.getFloat32(field_id));
                    break;
                case FLOAT64:
                    keyBytes =  Bytes.toBytes(key.getFloat64(field_id));
                    default:
            }
        }
        if (keyBytes != null)
            hBaseClient.write(tableName, new Delete(keyBytes));
    }

    private String tableName(String topic) {
        if (config.getTableName().isEmpty()) {
            return topic;
        }else {
            return config.getTableName();
        }
    }

//    private Struct extractData(SinkRecord sinkRecord, String dataField){
//        Object value = sinkRecord.value();
//        if (! (value instanceof Struct) ) {
//            throw new DataException("SinkRecord's value is not of struct type.");
//        }
//        if (null != dataField && dataField.length() > 0) {
//            value = ((Struct) value).getStruct(dataField);
//            if (!(value instanceof Struct)) {
//                logger.warn("Value's data field is not of struct type.");
//            }
//        }
//        return (Struct) value;
//    }

    private Struct extractData(SinkRecord sinkRecord) {

        Object value = sinkRecord.value();
        String dataField = config.getDataField();
        if (! (value instanceof Struct) ) {
            throw new DataException("SinkRecord's value is not of struct type.");
        }
        if (dataField.length() > 0) {
            value = ((Struct) value).getStruct(dataField);
            if (! (value instanceof Struct) ) { // mysql delete struct 'after' is null;
                throw new DataException("Value's data field is not of struct type.");
                //logger.warn("Value's data field is not of struct type.");
            }
        }

        return (Struct) value;

    }

    private byte[] getFieldData(Struct data, String field) {
        if (data.get(field) == null) {
            return null;
        }
        Schema schema = data.schema();
        Transformer transformer = null;
        if (MapUtils.isNotEmpty(config.getTransformerMapping())) {
            transformer = config.getTransformerMapping().get(field);
        }
        if (transformer != null) {
            Object value;
            switch (schema.field(field).schema().type()) {
                case BOOLEAN:
                    value = transformer.transform(data.getBoolean(field));
                    break;
                case STRING:
                     value = transformer.transform(data.getString(field));
                    break;
                case BYTES:
                    value = transformer.transform(data.getBytes(field));
                    break;
                case INT8:
                    value = transformer.transform(data.getInt8(field));
                    break;
                case INT16:
                    value = transformer.transform(data.getInt16(field));
                    break;
                case INT32:
                    value = transformer.transform(data.getInt32(field));
                    break;
                case INT64:
                    value = transformer.transform(data.getInt64(field));
                    break;
                case FLOAT32:
                    value = transformer.transform(data.getFloat32(field));
                    break;
                case FLOAT64:
                    value = transformer.transform(data.getFloat64(field));
                    break;
                default:
                    return null;
            }
            if (value instanceof Byte) {
                return Bytes.toBytes((Byte) value);
            } else if (value instanceof Short) {
                return Bytes.toBytes((Short) value);
            } else if (value instanceof Integer) {
                return Bytes.toBytes((Integer) value);
            } else if (value instanceof Long) {
                return Bytes.toBytes((Long) value);
            } else if (value instanceof byte[]) {
                return (byte[]) value;
            } else if (value instanceof Boolean) {
                return Bytes.toBytes((Boolean) value);
            } else if (value instanceof String) {
                return Bytes.toBytes((String) value);
            } else if (value instanceof Float) {
                return Bytes.toBytes((Float) value);
            } else if (value instanceof Double) {
                return Bytes.toBytes((Double) value);
            } else {
                return null;
            }
        } else {
            switch (schema.field(field).schema().type()) {
                case BOOLEAN:
                    return Bytes.toBytes(data.getBoolean(field));
                case STRING:
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
    }


//    private static byte[] getFieldData(Struct data, String field) {
//        if (data.get(field) == null) {
//            return null;
//        }
//        Schema schema = data.schema();
//        switch(schema.field(field).schema().type()) {
//        case BOOLEAN:
//            return Bytes.toBytes(data.getBoolean(field));
//        case STRING:
//            return Bytes.toBytes(data.getString(field));
//        case BYTES:
//            return data.getBytes(field);
//        case INT8:
//            return Bytes.toBytes(data.getInt8(field));
//        case INT16:
//            return Bytes.toBytes(data.getInt16(field));
//        case INT32:
//            return Bytes.toBytes(data.getInt32(field));
//        case INT64:
//            return Bytes.toBytes(data.getInt64(field));
//        case FLOAT32:
//            return Bytes.toBytes(data.getFloat32(field));
//        case FLOAT64:
//            return Bytes.toBytes(data.getFloat64(field));
//        default:
//            return null;
//        }
//    }

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
        if (rowKey == null || rowKey.length == 0)   // for data that has't not primary key
            return null;
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
