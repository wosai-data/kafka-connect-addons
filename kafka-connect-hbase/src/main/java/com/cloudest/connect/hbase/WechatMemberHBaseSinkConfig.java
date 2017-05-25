package com.cloudest.connect.hbase;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class WechatMemberHBaseSinkConfig extends AbstractConfig {


    public static final String ZOOKEEPER_QUORUM_CONFIG = "zookeeper.quorum";
    public static final String ZOOKEEPER_QUORUM_DOC = "Zookeeper quorums, e.g. s1-zk-001:2181,s1-zk-002:2181";


    public static final String HBASE_TABLE_NAME_CONFIG = "hbase.table.name";
    public static final String HBASE_TABLE_NAME_DOC = "HBase table name";
    //public static final String HBASE_TABLE_NAME_DEFAULT = "";



    private static ConfigDef config = new ConfigDef();

    static {

        config.define(ZOOKEEPER_QUORUM_CONFIG, Type.STRING, Importance.HIGH, ZOOKEEPER_QUORUM_DOC);
        config.define(HBASE_TABLE_NAME_CONFIG, Type.STRING, Importance.HIGH, HBASE_TABLE_NAME_DOC);

    }

    public static ConfigDef getConfig() {
        return config;
    }

    public WechatMemberHBaseSinkConfig(Map<String, String> originals) {
        super(config, originals);

    }


    public String getZookeeperQuorum() {
        return getString(ZOOKEEPER_QUORUM_CONFIG);
    }

    public String getTableName() {
        return getString(HBASE_TABLE_NAME_CONFIG);
    }

    public String getSubTableName() {
    	return getTableName() + "_sub";
    }
}
