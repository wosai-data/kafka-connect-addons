package com.cloudest.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class FieldTimeDailyPartitioner extends FieldTimeBasedPartitioner {

    private static long oneDayInMs = TimeUnit.HOURS.toMillis(24);
    private static String defaultPathFormat = "YYYY/MM/dd/";
    private static String defaultTZ = "Asia/Shanghai";
    private static String defaultLocale = "en_US";
    private static String defaultFieldName = "mtime";

    @Override
    public void configure(Map<String, Object> config) {
        if (config.get(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG) == null) {
            config.put(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, defaultPathFormat);
        }
        if (config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG) == null) {
            config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, defaultLocale);
        }
        if (config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG) == null) {
            config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, defaultTZ);
        }
        if (config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG) == null) {
            config.put(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, defaultFieldName);
        }
        config.put(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, oneDayInMs);
        super.configure(config);
    }

  }
