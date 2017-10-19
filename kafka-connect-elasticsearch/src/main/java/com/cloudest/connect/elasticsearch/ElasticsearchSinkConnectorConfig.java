package com.cloudest.connect.elasticsearch;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {
    // Connector configs
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC =
            "List of Elasticsearch HTTP connection URLs e.g. ``http://eshost1:9200,"
                    + "http://eshost2:9200``.";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC =
            "The number of records to process as a batch when writing to Elasticsearch. " +
                    "Default is " + ElasticsearchSinkConnectorConstants.BATCH_SIZE_DEFAULT + ".";
    public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
    private static final String MAX_IN_FLIGHT_REQUESTS_DOC =
            "The maximum number of indexing requests that can be in-flight to Elasticsearch before "
                    + "blocking further requests. "
                    + "Default is " + ElasticsearchSinkConnectorConstants.MAX_IN_FLIGHT_REQUESTS_DEFAULT + ".";
    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    private static final String MAX_BUFFERED_RECORDS_DOC =
            "The maximum number of records each task will buffer before blocking acceptance of more "
                    + "records. This config can be used to limit the memory usage for each task. "
                    + "Default is " + ElasticsearchSinkConnectorConstants.MAX_BUFFERED_RECORDS_DEFAULT + ".";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC =
            "Linger time in milliseconds for batching.\n"
                    + "Records that arrive in between request transmissions are batched into a single bulk "
                    + "indexing request, based on the ``" + BATCH_SIZE_CONFIG + "`` configuration. Normally "
                    + "this only occurs under load when records arrive faster than they can be sent out. "
                    + "However it may be desirable to reduce the number of requests even under light load and "
                    + "benefit from bulk indexing. This setting helps accomplish that - when a pending batch is"
                    + " not full, rather than immediately sending it out the task will wait up to the given "
                    + "delay to allow other records to be added so that they can be batched into a single "
                    + "request. Default is " + ElasticsearchSinkConnectorConstants.LINGER_MS_DEFAULT + ".";
    public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
    private static final String FLUSH_TIMEOUT_MS_DOC =
            "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer "
                    + "space to be made available by completed requests as records are added. If this timeout "
                    + "is exceeded the task will fail. "
                    + "Default is " + ElasticsearchSinkConnectorConstants.FLUSH_TIMEOUT_MS_DEFAULT + ".";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String MAX_RETRIES_DOC =
            "The maximum number of retries that are allowed for failed indexing requests. If the retry "
                    + "attempts are exhausted the task will fail. "
                    + "Default is " + ElasticsearchSinkConnectorConstants.MAX_RETRIES_DEFAULT + ".";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC =
            "How long to wait in milliseconds before attempting the first retry of a failed indexing "
                    + "request. Upon a failure, this connector may wait up to twice as long as the previous "
                    + "wait, up to the maximum number of retries. "
                    + "This avoids retrying in a tight loop under failure scenarios. "
                    + "Default is " + ElasticsearchSinkConnectorConstants.RETRY_BACKOFF_MS + ".";

    // Conversion configs
    public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
    private static final String TOPIC_INDEX_MAP_DOC =
            "A map from Kafka topic name to the destination Elasticsearch index, represented as a list "
                    + "of ``topic:index`` pairs.";
    public static final String TOPIC_TYPE_MAP_CONFIG = "topic.type.map";
    private static final String TOPIC_TYPE_MAP_DOC =
            "A map from Kafka topic name to the destination Elasticsearch type, represented as a list "
                    + "of ``topic:type`` pairs.";
    public static final String ID_DELIMITER_CONFIG = "id.delimiter";
    private static final String ID_DELIMITER_DOC =
            "When a record's primary key contains multiple fields, delimiter will be used " +
                    "when concatenating key fields into a single document id.";
    public static final String DATA_FIELD_CONFIG = "data.field";
    private static final String DATA_FIELD_DOC =
            "The field name from which record data will be extracted. " +
                    "When left empty, the message value itself will be extracted. Default is ``after``";
    public static final String VERSION_FIELD_CONFIG = "version.field";
    private static final String VERSION_FIELD_DOC =
            "The filed name from which record version will be extracted. " +
                    "The type of version field must be integer or long. Default is ``version``";
    public static final String DROP_INVALID_MESSAGE_CONFIG = "drop.invalid.message";
    private static final String DROP_INVALID_MESSAGE_DOC =
            "Whether to drop kafka message when it cannot be converted to output message.";

    public static final ConfigDef CONFIG = baseConfigDef();

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addConversionConfigs(configDef);
        return configDef;
    }

    private static void addConnectorConfigs(ConfigDef configDef) {
        final String group = "Connector";
        int order = 0;
        configDef.define(
                CONNECTION_URL_CONFIG,
                Type.LIST,
                Importance.HIGH,
                CONNECTION_URL_DOC,
                group,
                ++order,
                Width.LONG,
                "Connection URLs"
        ).define(
                BATCH_SIZE_CONFIG,
                Type.INT,
                ElasticsearchSinkConnectorConstants.BATCH_SIZE_DEFAULT,
                Importance.MEDIUM,
                BATCH_SIZE_DOC,
                group,
                ++order,
                Width.SHORT,
                "Batch Size"
        ).define(
                MAX_IN_FLIGHT_REQUESTS_CONFIG,
                Type.INT,
                ElasticsearchSinkConnectorConstants.MAX_IN_FLIGHT_REQUESTS_DEFAULT,
                Importance.MEDIUM,
                MAX_IN_FLIGHT_REQUESTS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Max In-flight Requests"
        ).define(
                MAX_BUFFERED_RECORDS_CONFIG,
                Type.INT,
                ElasticsearchSinkConnectorConstants.MAX_BUFFERED_RECORDS_DEFAULT,
                Importance.LOW,
                MAX_BUFFERED_RECORDS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Max Buffered Records"
        ).define(
                LINGER_MS_CONFIG,
                Type.LONG,
                ElasticsearchSinkConnectorConstants.LINGER_MS_DEFAULT,
                Importance.LOW,
                LINGER_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Linger (ms)"
        ).define(
                FLUSH_TIMEOUT_MS_CONFIG,
                Type.LONG,
                ElasticsearchSinkConnectorConstants.FLUSH_TIMEOUT_MS_DEFAULT,
                Importance.LOW,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                MAX_RETRIES_CONFIG,
                Type.INT,
                ElasticsearchSinkConnectorConstants.MAX_RETRIES_DEFAULT,
                Importance.LOW,
                MAX_RETRIES_DOC,
                group,
                ++order,
                Width.SHORT,
                "Max Retries"
        ).define(
                RETRY_BACKOFF_MS_CONFIG,
                Type.LONG,
                ElasticsearchSinkConnectorConstants.RETRY_BACKOFF_MS,
                Importance.LOW,
                RETRY_BACKOFF_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Retry Backoff (ms)"
        );
    }

    private static void addConversionConfigs(ConfigDef configDef) {
        final String group = "Data Conversion";
        int order = 0;
        configDef.define(
                TOPIC_INDEX_MAP_CONFIG,
                Type.LIST,
                "",
                Importance.MEDIUM,
                TOPIC_INDEX_MAP_DOC,
                group,
                ++order,
                Width.LONG,
                "Topic to Index Map"
        ).define(
                TOPIC_TYPE_MAP_CONFIG,
                Type.LIST,
                "",
                Importance.MEDIUM,
                TOPIC_TYPE_MAP_DOC,
                group,
                ++order,
                Width.LONG,
                "Topic to Type Map"
        ).define(
                ID_DELIMITER_CONFIG,
                Type.STRING,
                ElasticsearchSinkConnectorConstants.ID_DELIMITER_DEFAULT,
                Importance.MEDIUM,
                ID_DELIMITER_DOC,
                group,
                ++order,
                Width.SHORT,
                "ID Delimiter"
        ).define(
                DATA_FIELD_CONFIG,
                Type.STRING,
                ElasticsearchSinkConnectorConstants.DATA_FIELD_DEFAULT,
                Importance.HIGH,
                DATA_FIELD_DOC,
                group,
                ++order,
                Width.SHORT,
                "Field name for extracting record data"
        ).define(
                VERSION_FIELD_CONFIG,
                Type.STRING,
                ElasticsearchSinkConnectorConstants.VERSION_FIELD_DEFAULT,
                Importance.MEDIUM,
                VERSION_FIELD_DOC,
                group,
                ++order,
                Width.SHORT,
                "Field name for extracting record data version"
        ).define(
                DROP_INVALID_MESSAGE_CONFIG,
                Type.BOOLEAN,
                false,
                Importance.LOW,
                DROP_INVALID_MESSAGE_DOC,
                group,
                ++order,
                Width.LONG,
                "Drop invalid messages");
    }

    public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    public static void main(String[] args) {
//        System.out.println(CONFIG.toEnrichedRst());
        System.out.println(CONFIG.toRst());
    }
}
