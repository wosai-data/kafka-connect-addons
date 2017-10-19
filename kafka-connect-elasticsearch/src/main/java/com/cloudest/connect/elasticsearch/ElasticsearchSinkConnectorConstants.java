package com.cloudest.connect.elasticsearch;

import org.apache.kafka.connect.data.Schema.Type;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkConnectorConstants {
    public static final int BATCH_SIZE_DEFAULT = 2000;
    public static final int MAX_IN_FLIGHT_REQUESTS_DEFAULT = 5;
    public static final int MAX_BUFFERED_RECORDS_DEFAULT = 20000;
    public static final long LINGER_MS_DEFAULT = 1L;
    public static final long FLUSH_TIMEOUT_MS_DEFAULT = 10000L;
    public static final int MAX_RETRIES_DEFAULT = 5;
    public static final long RETRY_BACKOFF_MS = 100L;

    public static final String DATA_FIELD_DEFAULT = "after";
    public static final String VERSION_FIELD_DEFAULT = "version";
    public static final String ID_DELIMITER_DEFAULT = ":";

    public static final String MAP_KEY = "key";
    public static final String MAP_VALUE = "value";

    public static final String BOOLEAN_TYPE = "boolean";
    public static final String BYTE_TYPE = "byte";
    public static final String BINARY_TYPE = "binary";
    public static final String SHORT_TYPE = "short";
    public static final String INTEGER_TYPE = "integer";
    public static final String LONG_TYPE = "long";
    public static final String FLOAT_TYPE = "float";
    public static final String DOUBLE_TYPE = "double";
    public static final String STRING_TYPE = "string";
    public static final String DATE_TYPE = "date";

    static final Map<Type, String> TYPES = new HashMap<>();

    static {
        TYPES.put(Type.BOOLEAN, BOOLEAN_TYPE);
        TYPES.put(Type.INT8, BYTE_TYPE);
        TYPES.put(Type.INT16, SHORT_TYPE);
        TYPES.put(Type.INT32, INTEGER_TYPE);
        TYPES.put(Type.INT64, LONG_TYPE);
        TYPES.put(Type.FLOAT32, FLOAT_TYPE);
        TYPES.put(Type.FLOAT64, DOUBLE_TYPE);
        TYPES.put(Type.STRING, STRING_TYPE);
        TYPES.put(Type.BYTES, BINARY_TYPE);
    }
}
