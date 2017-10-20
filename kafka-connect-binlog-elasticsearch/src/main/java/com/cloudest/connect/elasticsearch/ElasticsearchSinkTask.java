package com.cloudest.connect.elasticsearch;

import com.cloudest.connect.elasticsearch.writer.ElasticsearchWriter;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ElasticsearchSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
    private ElasticsearchWriter writer;
    private JestClient client;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        start(props, null);
    }

    private void start(Map<String, String> props, JestClient client) {
        try {
            logger.info("Starting ElasticsearchSinkTask.");

            ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

            Map<String, String> topicToIndexMap =
                    parseMapConfig(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG));
            Map<String, String> topicToTypeMap =
                    parseMapConfig(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_TYPE_MAP_CONFIG));
            String idDelimiter = config.getString(ElasticsearchSinkConnectorConfig.ID_DELIMITER_CONFIG);
            String dataField = config.getString(ElasticsearchSinkConnectorConfig.DATA_FIELD_CONFIG);
            String versionField = config.getString(ElasticsearchSinkConnectorConfig.VERSION_FIELD_CONFIG);

            long flushTimeoutMs =
                    config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
            int maxBufferedRecords =
                    config.getInt(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
            int batchSize =
                    config.getInt(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
            long lingerMs =
                    config.getLong(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
            int maxInFlightRequests =
                    config.getInt(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
            long retryBackoffMs =
                    config.getLong(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
            int maxRetry =
                    config.getInt(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
            boolean dropInvalidMessage =
                    config.getBoolean(ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG);

            // Calculate the maximum possible backoff time ...
            long maxRetryBackoffMs = RetryUtil.computeRetryWaitTimeInMillis(maxRetry, retryBackoffMs);
            if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
                logger.warn("This connector uses exponential backoff with jitter for retries, "
                                + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                                + "backoff time greater than {} hours.",
                        ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, maxRetry,
                        ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs,
                        TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
            }

            if (client != null) {
                this.client = client;
            } else {
                List<String> addresses =
                        config.getList(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);
                JestClientFactory factory = new JestClientFactory();
                factory.setHttpClientConfig(
                        new HttpClientConfig.Builder(addresses)
                                .multiThreaded(true)
                                .build()
                );
                this.client = factory.getObject();
            }

            ElasticsearchWriter.Builder builder = new ElasticsearchWriter.Builder(this.client)
                    .setIdDelimiter(idDelimiter)
                    .setDataField(dataField)
                    .setVersionField(versionField)
                    .setTopicToIndexMap(topicToIndexMap)
                    .setTopicToTypeMap(topicToTypeMap)
                    .setFlushTimeoutMs(flushTimeoutMs)
                    .setMaxBufferedRecords(maxBufferedRecords)
                    .setMaxInFlightRequests(maxInFlightRequests)
                    .setBatchSize(batchSize)
                    .setLingerMs(lingerMs)
                    .setRetryBackoffMs(retryBackoffMs)
                    .setMaxRetry(maxRetry)
                    .setDropInvalidMessage(dropInvalidMessage);

            writer = builder.build();
            writer.start();
        } catch (ConfigException e) {
            throw new ConnectException(
                    "Couldn't start ElasticsearchSinkTask due to configuration error:",
                    e
            );
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        logger.debug("Opening the task for topic partitions: {}", partitions);
        Set<String> topics = new HashSet<>();
        for (TopicPartition tp : partitions) {
            topics.add(tp.topic());
        }
        writer.createIndicesForTopics(topics);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        logger.trace("Putting {} to Elasticsearch.", records);
        writer.write(records);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.trace("Flushing data to Elasticsearch with the following offsets: {}", offsets);
        writer.flush();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        logger.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public void stop() throws ConnectException {
        logger.info("Stopping ElasticsearchSinkTask.");
        if (writer != null) {
            writer.stop();
        }
        if (client != null) {
            client.shutdownClient();
        }
    }

    private Map<String, String> parseMapConfig(List<String> values) {
        Map<String, String> map = new HashMap<>();
        for (String value: values) {
            String[] parts = value.split(":");
            String topic = parts[0];
            String type = parts[1];
            map.put(topic, type);
        }
        return map;
    }
}
