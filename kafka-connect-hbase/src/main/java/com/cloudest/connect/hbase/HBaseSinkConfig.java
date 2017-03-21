package com.cloudest.connect.hbase;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class HBaseSinkConfig extends AbstractConfig {

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

    private String[] rowkeyColumns;

    private static ConfigDef config = new ConfigDef();

    static {

        config.define(DATA_FIELD_CONFIG, Type.STRING, DATA_FIELD_DEFAULT, Importance.HIGH, DATA_FIELD_DOC);

        config.define(ZOOKEEPER_QUORUM_CONFIG, Type.STRING, Importance.HIGH, ZOOKEEPER_QUORUM_DOC);
        config.define(HBASE_ROWKEY_COLUMNS_CONFIG, Type.STRING, HBASE_ROWKEY_COLUMNS_DEFAULT, Importance.HIGH, HBASE_ROWKEY_COLUMNS_DOC);
        config.define(HBASE_ROWKEY_DELIMITER_CONFIG, Type.STRING, HBASE_ROWKEY_DELIMITER_DEFAULT, Importance.HIGH, HBASE_ROWKEY_DELIMITER_DOC);
        config.define(HBASE_COLUMN_FAMILY_CONFIG, Type.STRING, HBASE_COLUMN_FAMILY_DEFAULT, Importance.HIGH, HBASE_COLUMN_FAMILY_DOC);
        config.define(HBASE_TABLE_NAME_CONFIG, Type.STRING, HBASE_TABLE_NAME_DEFAULT, Importance.HIGH, HBASE_TABLE_NAME_DOC);

    }
    public static ConfigDef getConfig() {
        return config;
    }

    public HBaseSinkConfig(Map<String, String> originals) {
        super(config, originals);

        this.rowkeyColumns = getString(HBASE_ROWKEY_COLUMNS_CONFIG).split(",");
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
}
