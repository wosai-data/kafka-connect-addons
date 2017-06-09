package com.cloudest.connect.hbase;

import com.cloudest.connect.hbase.transformer.Transformer;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HBaseSinkConfig extends AbstractConfig {

    private static final Logger logger = LoggerFactory.getLogger(HBaseSinkConfig.class);


    public static final String DATA_FIELD_CONFIG = "data.field";
    public static final String DATA_FIELD_DOC = "Name of the field where the record data nests.";
    public static final String DATA_FIELD_DEFAULT = "";

    public static final String ZOOKEEPER_QUORUM_CONFIG = "zookeeper.quorum";
    public static final String ZOOKEEPER_QUORUM_DOC = "Zookeeper quorums, e.g. s1-zk-001:2181,s1-zk-002:2181";

    public static final String HBASE_ROWKEY_COLUMNS_CONFIG = "hbase.rowkey.columns";
    public static final String HBASE_ROWKEY_COLUMNS_DOC = "Names of columns whose values will be used to forumate rowkey, e.g. first_name,last_name";
    public static final String HBASE_ROWKEY_COLUMNS_DEFAULT = "id";

    public static final String HBASE_ROWKEY_DELIMITER_CONFIG = "hbase.rowkey.delimiter";
    public static final String HBASE_ROWKEY_DELIMITER_DOC = "Delimiter to be used to separate columns in the generated rowkey, e.g. ,";
    public static final String HBASE_ROWKEY_DELIMITER_DEFAULT = ",";

    public static final String HBASE_COLUMN_FAMILY_CONFIG = "hbase.column.family";
    public static final String HBASE_COLUMN_FAMILY_DOC = "HBase column family into which the records will be saved";
    public static final String HBASE_COLUMN_FAMILY_DEFAULT = "d";

    public static final String HBASE_TABLE_NAME_CONFIG = "hbase.table.name";
    public static final String HBASE_TABLE_NAME_DOC = "HBase table name";
    public static final String HBASE_TABLE_NAME_DEFAULT = "";

    public static final String FIELDS_TRANSFORMERS_CONFIG = "field.transformers";
    public static final String FIELDS_TRANSFORMERS_DOC = "field transformer mapping: field1=transformerClass1,field2=transformerClass2,...";
    public static final String FIELDS_TRANSFORMERS_DEFAULT = "";

    private String[] rowkeyColumns;

    private static ConfigDef config = new ConfigDef();

    private Map<String, Transformer> transformerMapping = new HashMap<>();

    static {

        config.define(DATA_FIELD_CONFIG, Type.STRING, DATA_FIELD_DEFAULT, Importance.HIGH, DATA_FIELD_DOC);

        config.define(ZOOKEEPER_QUORUM_CONFIG, Type.STRING, Importance.HIGH, ZOOKEEPER_QUORUM_DOC);
        config.define(HBASE_ROWKEY_COLUMNS_CONFIG, Type.STRING, HBASE_ROWKEY_COLUMNS_DEFAULT, Importance.HIGH, HBASE_ROWKEY_COLUMNS_DOC);
        config.define(HBASE_ROWKEY_DELIMITER_CONFIG, Type.STRING, HBASE_ROWKEY_DELIMITER_DEFAULT, Importance.HIGH, HBASE_ROWKEY_DELIMITER_DOC);
        config.define(HBASE_COLUMN_FAMILY_CONFIG, Type.STRING, HBASE_COLUMN_FAMILY_DEFAULT, Importance.HIGH, HBASE_COLUMN_FAMILY_DOC);
        config.define(HBASE_TABLE_NAME_CONFIG, Type.STRING, HBASE_TABLE_NAME_DEFAULT, Importance.HIGH, HBASE_TABLE_NAME_DOC);

        config.define(FIELDS_TRANSFORMERS_CONFIG, Type.STRING, FIELDS_TRANSFORMERS_DEFAULT, Importance.HIGH, FIELDS_TRANSFORMERS_DOC); // add by terry
    }
    public static ConfigDef getConfig() {
        return config;
    }

    public HBaseSinkConfig(Map<String, String> originals) {
        super(config, originals);

        this.rowkeyColumns = getString(HBASE_ROWKEY_COLUMNS_CONFIG).split(",");
        initTransformers();
    }

    public String[] getRowkeyColumns() {
        return rowkeyColumns;
    }

    public String getDataField() {
        return getString(DATA_FIELD_CONFIG);
    }

    public String getZookeeperQuorum() {
        return getString(ZOOKEEPER_QUORUM_CONFIG);
    }

    public String getRowkeyDelimiter() {
        return getString(HBASE_ROWKEY_DELIMITER_CONFIG);
    }

    public String getColumnFamily() {
        return getString(HBASE_COLUMN_FAMILY_CONFIG);
    }

    public String getTableName() {
        return getString(HBASE_TABLE_NAME_CONFIG);
    }

    public String getFieldsTransformers () {
        return getString(FIELDS_TRANSFORMERS_CONFIG);
    }

    public Map<String, Transformer> getTransformerMapping() {
        return transformerMapping;
    }

    private void initTransformers() {
        String transformerMappingStr = getFieldsTransformers();
        if (StringUtils.isNotBlank(transformerMappingStr)) {
            Map<String, String> fieldTransformers = parseFieldTransformer(transformerMappingStr);
            if (MapUtils.isNotEmpty(fieldTransformers)){
                for (String fieldName: fieldTransformers.keySet()){
                    try {
                        transformerMapping.put(fieldName, (Transformer) Utils.newInstance(Class.forName(fieldTransformers.get(fieldName))));
                    } catch (ClassNotFoundException e) {
                        logger.error("class not found:{}", fieldTransformers.get(fieldName), e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private Map<String, String> parseFieldTransformer(String transformerMappingStr) {
        Map<String, String> fieldTransformers = new HashMap<>();
        if (StringUtils.isNotBlank(transformerMappingStr)) {
            String[] keyValues = StringUtils.split(transformerMappingStr, ",");
            if (keyValues != null && keyValues.length > 0) {
                for (String keyValue : keyValues) {
                    if (StringUtils.contains(keyValue, "=")) {
                        String[] keyAndValue = StringUtils.split(keyValue, "=");
                        fieldTransformers.put(keyAndValue[0], keyAndValue[1]);
                    }
                }
            }
        }
        return fieldTransformers;
    }


}
