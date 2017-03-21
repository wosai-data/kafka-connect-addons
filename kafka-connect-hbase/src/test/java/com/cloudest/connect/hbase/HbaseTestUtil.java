package com.cloudest.connect.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public abstract class HbaseTestUtil {

    private static HBaseTestingUtility utility;

    /**
     * Returns a new HBaseTestingUtility instance.
     */
    private static HBaseTestingUtility createTestingUtility() {
        final Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        return new HBaseTestingUtility(hbaseConf);
    }

    public static HBaseTestingUtility getUtility() {
        if (utility == null) {
            utility = createTestingUtility();
        }
        return utility;
    }

    public static void startMiniCluster() {
        try {
            getUtility().startMiniCluster();
        } catch (Exception e) {
            throw new RuntimeException("error starting hbase mini cluster.", e);
        }
    }

    public static void stopMiniCluster() {
        if (utility != null) {
            try {
                utility.shutdownMiniCluster();
            } catch (Exception e) {
                throw new RuntimeException("error shutting down hbase mini cluster.", e);
            }
        }
    }

    public static void createTable(String tableName, String... columnFamilies) {
        final TableName name = TableName.valueOf(tableName);
        try (HBaseAdmin hBaseAdmin = utility.getHBaseAdmin()) {
            final HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            for (String family : columnFamilies) {
                final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            hBaseAdmin.createTable(hTableDescriptor);
            utility.waitUntilAllRegionsAssigned(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
