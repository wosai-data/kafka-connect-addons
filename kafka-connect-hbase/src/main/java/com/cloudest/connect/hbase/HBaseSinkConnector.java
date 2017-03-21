
package com.cloudest.connect.hbase;

import com.google.common.collect.Lists;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

public class HBaseSinkConnector extends SinkConnector {

    public static final String VERSION = "1.0";
    private Map<String, String> props;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HBaseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = Lists.newArrayList();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // NO-OP
    }

    @Override
    public ConfigDef config() {
        return HBaseSinkConfig.getConfig();
    }
}
