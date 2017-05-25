package com.cloudest.connect.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class WechatMemberHBaseSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(WechatMemberHBaseSinkTask.class);

    private HBaseClient hBaseClient;
    private WechatMemberHBaseSinkConfig config;
    private ObjectMapper om = new ObjectMapper();
    
    
    public static final String SUB = "sub";
    public static final String PUSH = "push";
    public static final String MEDIAS = "medias";
    public static final String DEVICE_TYPE = "device_type";
    public static final String HOT_IND = "hotInd";
    
    public static final String PUSH_CF = "p";
    public static final String MEDIAS_CF = "m";
    public static final String DEVICE_TYPE_CF = "dt";
    public static final String HOT_IND_CF = "hi";

    public static final String BASE_CF = "b";

    @Override
    public String version() {
        return HBaseSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new WechatMemberHBaseSinkConfig(props);

        try {
            hBaseClient = new HBaseClient(config.getZookeeperQuorum());
        } catch (IOException e) {
            throw new ConnectException("failed to connect to zookeeper quorum", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record: records) {
            String tableName = config.getTableName();
            String subTableName = config.getSubTableName();
            try {
            	List<Mutation> tableMutations = new ArrayList<>();
            	List<Mutation> subTableMutations = new ArrayList<>();
            	toMutations(record, tableMutations, subTableMutations);
            	
                hBaseClient.write(tableName, tableMutations);
                hBaseClient.write(subTableName, subTableMutations);
            } catch (IOException e) {
                throw new ConnectException("Failed to write record to hbase", e);
            }
        }
    }


    private Struct extractData(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (! (value instanceof Struct) ) {
            throw new DataException("SinkRecord's value is not of struct type.");
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

    private byte[] rowkey(Struct key, String field) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return getFieldData(key, field);
    }

    private byte[] getBytes(Object value) {
    	if (value == null){
    		return null;
    	}
    	if (value instanceof Integer) {
    		return Bytes.toBytes((Integer)value);
    	}else if (value instanceof Long) {
    		return Bytes.toBytes((Long)value);
    		
    	}else if (value instanceof Float) {
    		return Bytes.toBytes((Float)value);
    	}else if (value instanceof Double) {
    		return Bytes.toBytes((Double)value);
    	}else if (value instanceof Boolean) {
    		return Bytes.toBytes((Boolean)value);
    	}else if (value instanceof String) {
    		return Bytes.toBytes((String)value);
    	}else {
    		logger.warn("unsupported primitive type {}", value.getClass().getName());
    		return null;
    	}
    }
    private void setComplexFieldArray(byte[] rowkey, List<Object> arr, String cf, List<Mutation> mutations) {
        
    	Delete deleteAll = new Delete(rowkey);
    	deleteAll.addFamily(Bytes.toBytes(cf), System.currentTimeMillis());
    	mutations.add(deleteAll);
    	

    	if (arr != null && !arr.isEmpty()) {

    		Put put = new Put(rowkey);

    		for(int i=0; i<arr.size(); ++i) {
    			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(i)), getBytes(arr.get(i)));
    		}
    		mutations.add(put);
    	}
    }
    private void setComplexFieldArrayElement(byte[] rowkey, String index, Object value, String cf, List<Mutation> mutations) {
    	if (value == null) {
    		Delete delete = new Delete(rowkey);
    		delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(index));
    		mutations.add(delete);
    	}else {
    		Put put = new Put(rowkey);
    		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(index), getBytes(value));
    		mutations.add(put);
    	}
    }
    private void setComplexFieldMap(byte[] rowkey, Map<String, Object> m, String cf, List<Mutation> mutations) {
        
    	Delete deleteAll = new Delete(rowkey);
    	deleteAll.addFamily(Bytes.toBytes(cf), System.currentTimeMillis());
    	mutations.add(deleteAll);
    	
    	if (m!=null && !m.isEmpty()) {
    		Put put = new Put(rowkey);
    		for (String i: m.keySet()) {
    			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(i), getBytes(m.get(i)));
    		}
    		mutations.add(put);
    	}
    }
    private void setComplexFieldMapEntry(byte[] rowkey, String index, Object value, String cf, List<Mutation> mutations) {
    	if (value == null) {
    		Delete delete = new Delete(rowkey);
    		delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(index));
    		mutations.add(delete);
    	}else {
    		Put put = new Put(rowkey);
    		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(index), getBytes(value));
    		mutations.add(put);
    	}
    }
    private void setFieldValue(byte[] rowkey, Object value, String cf, String qualifier, List<Mutation> mutations) {
    	if (value == null) {
    		Delete delete = new Delete(rowkey);
    		delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
    		mutations.add(delete);
    	}else {
    		Put put = new Put(rowkey);
    		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), getBytes(value));
    		mutations.add(put);
    	}
    }

    private static byte[] subRowkey(byte[] rowkey, String channel) throws IOException {
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	baos.write(rowkey);
    	baos.write(Bytes.toBytes("/"));
    	baos.write(Bytes.toBytes(channel));

    	return baos.toByteArray();
    }
    private void recordToSubTableMutations(byte[] rowkey, String channel, Map<String, Object> record, List<Mutation> subTableMutations) throws IOException {
    	byte[] subRowkey = subRowkey(rowkey, channel);
    	
    	for (String field: record.keySet()) {
    		if (PUSH.equals(field)) {
    			setComplexFieldMap(subRowkey, (Map<String,Object>)record.get(field), PUSH_CF, subTableMutations);
    		} else if (MEDIAS.equals(field)) {
    			setComplexFieldArray(subRowkey, (List<Object>)record.get(field), MEDIAS_CF, subTableMutations);
    		} else {
    			setFieldValue(subRowkey, record.get(field), BASE_CF, field, subTableMutations);
    		}
    	}
    }

    private void patchToMutations(byte[] rowkey, Map update, List<Mutation> tableMutations, List<Mutation> subTableMutations) throws IOException {
    	Map<String, Object> set = (Map<String, Object>)update.get("$set");
    	Map<String, Object> unset = (Map<String, Object>)update.get("$unset");
    	
    	if (set != null) {
    		for(String field: set.keySet()) {
    			String[] parts = field.split("\\.");
    			if (SUB.equals(parts[0])) {
    				if (parts.length == 1) {
    					Map<String, Object> subs = (Map<String, Object>)set.get(field);
    					for (String channel: subs.keySet()) {
    						recordToSubTableMutations(rowkey, channel, (Map<String, Object>)subs.get(channel), subTableMutations);
    					}
    				}else {
    					String channel = parts[1];
    					if (parts.length == 2) {
    						recordToSubTableMutations(rowkey, channel, (Map<String, Object>)set.get(field), subTableMutations);
    					}else {
    						String subField = parts[2];
    						if(parts.length ==3) {
    							if (PUSH.equals(subField)) {
    								setComplexFieldMap(subRowkey(rowkey, channel), (Map<String,Object>)set.get(field), PUSH_CF, subTableMutations);
    							}else if (MEDIAS.equals(subField)) {
    								setComplexFieldArray(subRowkey(rowkey, channel), (List<Object>)set.get(field), MEDIAS_CF, subTableMutations);
    							}else {
    								setFieldValue(subRowkey(rowkey, channel), set.get(field), BASE_CF, subField, subTableMutations);
    							}
    						}else if (parts.length == 4){
    							String index = parts[3];
    							if (PUSH.equals(subField)) {
    								setComplexFieldMapEntry(subRowkey(rowkey, channel), index, set.get(field), PUSH_CF, subTableMutations);
    							}else if (MEDIAS.equals(subField)){
    								setComplexFieldArrayElement(subRowkey(rowkey, channel), index, set.get(field), MEDIAS_CF, subTableMutations);
    							}
    						}else{

    						}
    					}
    				}

    			}else if (HOT_IND.equals(parts[0])) {
    				if (parts.length == 1) {
    					setComplexFieldArray(rowkey, (List<Object>)set.get(field), HOT_IND_CF, tableMutations);
    				}else if (parts.length == 2) {
    					setComplexFieldArrayElement(rowkey, parts[1], set.get(field), HOT_IND_CF, tableMutations);
    				}

    			}else if (DEVICE_TYPE.equals(parts[0])) {
    				if (parts.length == 1) {
    					setComplexFieldArray(rowkey, (List<Object>)set.get(field), DEVICE_TYPE_CF, tableMutations);
    				}else if (parts.length == 2) {
    					setComplexFieldArrayElement(rowkey, parts[1], set.get(field), DEVICE_TYPE_CF, tableMutations);
    				}
    			}else if (MEDIAS.equals(parts[0])) {
    				if (parts.length == 1) {
    					setComplexFieldArray(rowkey, (List<Object>)set.get(field), MEDIAS_CF, tableMutations);
    				}else if (parts.length == 2) {
    					setComplexFieldArrayElement(rowkey, parts[1], set.get(field), MEDIAS_CF, tableMutations);
    				}
    			}else {
    				if (parts.length == 1) {
    					setFieldValue(rowkey, set.get(field), BASE_CF, field, tableMutations);
    				} else {
    					logger.warn("bad field {}", field);
    				}
    			}
    		}
    	}
    }

    private void recordToMutations(byte[] rowkey, Map<String, Object> full, List<Mutation> tableMutations, List<Mutation> subTableMutations) throws IOException {
    	for(String field: full.keySet()) {
    		if (SUB.equals(field)) {
    			Map<String, Object> subs = (Map<String,Object>)full.get(field);
    			for(String channel: subs.keySet()) {
    				recordToSubTableMutations(rowkey, channel, (Map<String, Object>)subs.get(channel), subTableMutations);
    			}
    		}else if (HOT_IND.equals(field)) {
    			setComplexFieldArray(rowkey, (List<Object>)full.get(field), HOT_IND_CF, tableMutations);
    		}else if (DEVICE_TYPE.equals(field)) {
    			setComplexFieldArray(rowkey, (List<Object>)full.get(field), DEVICE_TYPE_CF, tableMutations);
    		}else if (MEDIAS.equals(field)) {
    			setComplexFieldArray(rowkey, (List<Object>)full.get(field), MEDIAS_CF, tableMutations);
    		}else{
    			setFieldValue(rowkey, full.get(field), BASE_CF, field, tableMutations);
    		}
    	}
    }

    private static String getStringField(Struct value, String field) {
    	Schema schema = value.schema();
    	if (schema.field(field).schema().type() == Type.STRING) {
    		return value.getString(field);
    	} else {
    		return null;
    	}
    }

    private void toMutations(SinkRecord record, List<Mutation> tableMutations, List<Mutation> subTableMutations) throws IOException {
    	Struct key = (Struct)record.key();
    	Struct value = (Struct)record.value();
    
    	String id = getStringField(key, "_id");
    	String after = getStringField(value, "after");
    	String patch = getStringField(value, "patch");
    	
    	byte[] rowkey = getBytes(id);
    	if (after != null) {
    		try {
    			Map full = om.readValue(after, Map.class);
    			recordToMutations(rowkey, full, tableMutations, subTableMutations);
    		} catch(IOException ex) {
    			logger.warn("bad record string {}", after, ex);
    		}
    	} else if (patch != null) {
    		try {
    			Map update = om.readValue(patch, Map.class);
    			patchToMutations(rowkey, update, tableMutations, subTableMutations);
    		} catch(IOException ex) {
    			logger.warn("bad patch string {}", patch, ex);
    		}
    	} else{
    		logger.warn("missing after/patch in oplog {}", record);
    	}
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
