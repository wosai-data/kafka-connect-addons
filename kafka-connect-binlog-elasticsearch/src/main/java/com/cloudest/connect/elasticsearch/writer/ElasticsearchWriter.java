package com.cloudest.connect.elasticsearch.writer;

import com.cloudest.connect.elasticsearch.*;
import com.cloudest.connect.elasticsearch.bulk.BulkProcessor;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticsearchWriter {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchWriter.class);

    private final JestClient client;
    private String dataField;
    private String versionField;
    private String idDelimiter;
    private String sinkAction;
    private String sinkFieldPaths;
    private String sinkFieldPathsArg;
    private String indexKeyExpression;
    private String updateScriptId;
    private String updateOnDeleteScriptId;
    private List<String> stringListFields;
    private List<String> integerListFields;
    private List<String> bytesToStringFields;
    private int versionConflictRetries;
    private final Map<String, String> topicToIndexMap;
    private final Map<String, String> topicToTypeMap;
    private final long flushTimeoutMs;
    private final BulkProcessor<SinkableRecord, ?> bulkProcessor;
    private final boolean dropInvalidMessage;
    private final DataConverter converter;

    private final Set<String> existingMappings;

    ElasticsearchWriter(
            JestClient client,
            String dataField,
            String versionField,
            String idDelimiter,
            String sinkAction,
            String sinkFieldPaths,
            String sinkFieldPathsArg,
            String indexKeyExpression,
            String updateScriptId,
            String updateOnDeleteScriptId,
            List<String> stringListFields,
            List<String> integerListFields,
            List<String> bytesToStringFields,
            int versionConflictRetries,
            Map<String, String> topicToIndexMap,
            Map<String, String> topicToTypeMap,
            long flushTimeoutMs,
            int maxBufferedRecords,
            int maxInFlightRequests,
            int batchSize,
            long lingerMs,
            int maxRetries,
            long retryBackoffMs,
            boolean dropInvalidMessage
    ) {
        this.client = client;
        this.dataField = dataField;
        this.versionField = versionField;
        this.idDelimiter = idDelimiter;
        this.sinkAction = sinkAction;
        this.sinkFieldPaths = sinkFieldPaths;
        this.sinkFieldPathsArg = sinkFieldPathsArg;
        this.indexKeyExpression = indexKeyExpression;
        this.updateScriptId = updateScriptId;
        this.updateOnDeleteScriptId = updateOnDeleteScriptId;
        this.stringListFields = stringListFields;
        this.integerListFields = integerListFields;
        this.bytesToStringFields = bytesToStringFields;
        this.versionConflictRetries = versionConflictRetries;
        this.topicToIndexMap = topicToIndexMap;
        this.topicToTypeMap = topicToTypeMap;
        this.flushTimeoutMs = flushTimeoutMs;
        this.dropInvalidMessage = dropInvalidMessage;
        this.converter = new DataConverter();

        bulkProcessor = new BulkProcessor<>(
                new SystemTime(),
                new BulkIndexingClient(client),
                maxBufferedRecords,
                maxInFlightRequests,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs
        );

        existingMappings = new HashSet<>();
    }

    public static class Builder {

        private final JestClient client;
        private String dataField;
        private String versionField;
        private String idDelimiter;
        private String sinkAction;
        private String sinkFieldPaths;
        private String sinkFieldPathsArg;
        private String indexKeyExpression;
        private String updateScriptId;
        private String updateOnDeleteScriptId;
        private List<String> stringListFields;
        private List<String> integerListFields;
        private List<String> bytesToStringFields;
        private int versionConflictRetries;
        private Map<String, String> topicToIndexMap = new HashMap<>();
        private Map<String, String> topicToTypeMap = new HashMap<>();
        private long flushTimeoutMs;
        private int maxBufferedRecords;
        private int maxInFlightRequests;
        private int batchSize;
        private long lingerMs;
        private int maxRetry;
        private long retryBackoffMs;
        private boolean dropInvalidMessage;

        public Builder(JestClient client) {
            this.client = client;
        }

        public Builder setDataField(String dataField) {
            this.dataField = dataField;
            return this;
        }

        public Builder setVersionField(String versionField) {
            this.versionField = versionField;
            return this;
        }

        public Builder setIdDelimiter(String idDelimiter) {
            this.idDelimiter = idDelimiter;
            return this;
        }

        public Builder setSinkAction(String sinkAction) {
            this.sinkAction = sinkAction;
            return this;
        }

        public Builder setSinkFieldPaths(String sinkFieldPaths) {
            this.sinkFieldPaths = sinkFieldPaths;
            return this;
        }

        public Builder setSinkFieldPathsArg(String sinkFieldPathsArg) {
            this.sinkFieldPathsArg = sinkFieldPathsArg;
            return this;
        }

        public Builder setIndexKeyExpression(String indexKeyExpression) {
            this.indexKeyExpression = indexKeyExpression;
            return this;
        }

        public Builder setUpdateScriptId(String updateScriptId) {
            this.updateScriptId = updateScriptId;
            return this;
        }

        public Builder setStringListFields(List<String> stringListFields) {
            this.stringListFields = stringListFields;
            return this;
        }

        public Builder setIntegerListFields(List<String> integerListFields) {
            this.integerListFields = integerListFields;
            return this;
        }

        public Builder setBytesToStringFields(List<String> bytesToStringFields) {
            this.bytesToStringFields = bytesToStringFields;
            return this;
        }

        public Builder setVersionConflictRetries(int versionConflictRetries) {
            this.versionConflictRetries = versionConflictRetries;
            return this;
        }

        public Builder setTopicToIndexMap(Map<String, String> topicToIndexMap) {
            this.topicToIndexMap = topicToIndexMap;
            return this;
        }

        public Builder setTopicToTypeMap(Map<String, String> topicToTypeMap) {
            this.topicToTypeMap = topicToTypeMap;
            return this;
        }

        public Builder setFlushTimeoutMs(long flushTimeoutMs) {
            this.flushTimeoutMs = flushTimeoutMs;
            return this;
        }

        public Builder setMaxBufferedRecords(int maxBufferedRecords) {
            this.maxBufferedRecords = maxBufferedRecords;
            return this;
        }

        public Builder setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setLingerMs(long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder setMaxRetry(int maxRetry) {
            this.maxRetry = maxRetry;
            return this;
        }

        public Builder setRetryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder setUpdateOnDeleteScriptId(String updateOnDeleteScriptId) {
            this.updateOnDeleteScriptId = updateOnDeleteScriptId;
            return this;
        }

        public Builder setDropInvalidMessage(boolean dropInvalidMessage) {
            this.dropInvalidMessage = dropInvalidMessage;
            return this;
        }

        public ElasticsearchWriter build() {
            return new ElasticsearchWriter(
                    client,
                    dataField,
                    versionField,
                    idDelimiter,
                    sinkAction,
                    sinkFieldPaths,
                    sinkFieldPathsArg,
                    indexKeyExpression,
                    updateScriptId,
                    updateOnDeleteScriptId,
                    stringListFields,
                    integerListFields,
                    bytesToStringFields,
                    versionConflictRetries,
                    topicToIndexMap,
                    topicToTypeMap,
                    flushTimeoutMs,
                    maxBufferedRecords,
                    maxInFlightRequests,
                    batchSize,
                    lingerMs,
                    maxRetry,
                    retryBackoffMs,
                    dropInvalidMessage
            );
        }

    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                SinkableRecord sinkableRecord = null;
//                if (!existingMappings.contains(key.index + "." + key.type)) {
//                    try {
//                        if (Mapping.getMapping(client, key.index, key.type) == null) {
//                            throw new ConnectException(
//                                    "No mapping was found for index: " + key.index + " and type: " + key.type
//                                            + ", please create mapping in elasticsearch first"
//                            );
//                        }
//                    } catch (IOException e) {
//                        throw new ConnectException(
//                                "Failed to retrieve mapping for index: " + key.index + " and type: " + key.type
//                                , e
//                        );
//                    }
//                    existingMappings.add(key.index + "." + key.type);
//                }
                Long version = extractDataVersion(record);
                Object payload = record.value();
                if (payload != null) {
                    Struct data = extractData(record);
                    if (data == null) return;
                    Key key = extractKey(record);
                    if (ElasticsearchSinkConnectorConstants.SINK_ACTION_INDEX.equals(this.sinkAction)) {
                        Map<String, Object> conversionConfigs = new HashMap<>();
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.STRING_LIST_FIELDS_CONFIG, stringListFields);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.INTEGER_LIST_FIELDS_CONFIG, integerListFields);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.BYTES_TO_STRING_FIELDS_CONFIG, bytesToStringFields);
                        sinkableRecord = converter.convertToSinkableRecord(data, SinkableRecord.Action.INDEX, key, version, conversionConfigs);
                    } else if (ElasticsearchSinkConnectorConstants.SINK_ACTION_UPSERT.equals(this.sinkAction)) {
                        Map<String, Object> conversionConfigs = new HashMap<>();
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_CONFIG, sinkFieldPaths);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_ARG_CONFIG, sinkFieldPathsArg);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.UPDATE_SCRIPT_ID_CONFIG, updateScriptId);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.STRING_LIST_FIELDS_CONFIG, stringListFields);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.INTEGER_LIST_FIELDS_CONFIG, integerListFields);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.BYTES_TO_STRING_FIELDS_CONFIG, bytesToStringFields);
                        conversionConfigs.put(ElasticsearchSinkConnectorConfig.VERSION_CONFLICT_RETRIES_CONFIG, String.valueOf(versionConflictRetries));
                        sinkableRecord = converter.convertToSinkableRecord(data, SinkableRecord.Action.UPSERT, key, version, conversionConfigs);
                    }
                } else if (ElasticsearchSinkConnectorConstants.SINK_ACTION_INDEX.equals(this.sinkAction)) { // for tombstone event, delete the document
                    Key key = extractKey(record);
                    Map<String, Object> conversionConfigs = new HashMap<>();
                    conversionConfigs.put(ElasticsearchSinkConnectorConfig.UPDATE_ON_DELETE_CONFIG, updateOnDeleteScriptId);
                    sinkableRecord = converter.convertToSinkableRecord(null, SinkableRecord.Action.DELETE, key, version, conversionConfigs);
                }
                if (sinkableRecord != null) {
                    bulkProcessor.add(sinkableRecord, flushTimeoutMs);
                }
            } catch (ConnectException convertException) {
                if (dropInvalidMessage) {
                    logger.error("Can't convert record from topic {} with partition {} and offset {}. Error message: {}",
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset(),
                            convertException.getMessage());
                } else {
                    throw convertException;
                }
            } catch (RuntimeException e) {
                logger.error("Failed to write to elasticsearch, record is {}", record, e);
                throw new ConnectException("Failed to write record to elasticsearch", e);
            }
        }
    }

    private Key extractKey(SinkRecord sinkRecord) {
        StringBuilder id = new StringBuilder();

        // Index and type
        String index;
        String type;
        final String indexOverride = topicToIndexMap.get(sinkRecord.topic());
        if (indexOverride != null) {
            index = indexOverride;
        } else {
            index = indexFromTopic(sinkRecord.topic());
        }
        final String typeOverride = topicToTypeMap.get(sinkRecord.topic());
        if (typeOverride != null) {
            type = typeOverride;
        } else {
            type = typeFromTopic(sinkRecord.topic());
        }

        // Id
        if (indexKeyExpression != null && !indexKeyExpression.isEmpty()
                && ElasticsearchSinkConnectorConstants.SINK_ACTION_UPSERT.equals(this.sinkAction)) {
            Struct data = extractData(sinkRecord);
            if (data != null) {
                id.append(StructUtil.evaluateFieldValue(data, indexKeyExpression));
            }
        } else {
            String delimiter = idDelimiter;
            if (idDelimiter == null) {
                idDelimiter = ElasticsearchSinkConnectorConstants.ID_DELIMITER_DEFAULT;
            }
            Object keyObj = sinkRecord.key();
            if (keyObj != null && keyObj instanceof Struct) {
                Struct key = (Struct) keyObj;
                for (Field field : key.schema().fields()) {
                    Schema.Type fieldType = key.schema().field(field.name()).schema().type();
                    if (!fieldType.isPrimitive()) continue;
                    switch (fieldType) {
                        case STRING:
                            id.append(key.getString(field.name()))
                                    .append(delimiter);
                            break;
                        case BYTES:
                            id.append(new String(key.getBytes(field.name())))
                                    .append(delimiter);
                            break;
                        case BOOLEAN:
                            id.append(key.getBoolean(field.name()))
                                    .append(delimiter);
                            break;
                        case FLOAT32:
                            id.append(key.getFloat32(field.name()))
                                    .append(delimiter);
                            break;
                        case FLOAT64:
                            id.append(key.getFloat64(field.name()))
                                    .append(delimiter);
                            break;
                        case INT8:
                            id.append(key.getInt8(field.name()))
                                    .append(delimiter);
                            break;
                        case INT16:
                            id.append(key.getInt16(field.name()))
                                    .append(delimiter);
                            break;
                        case INT32:
                            id.append(key.getInt32(field.name()))
                                    .append(delimiter);
                            break;
                        case INT64:
                            id.append(key.getInt64(field.name()))
                                    .append(delimiter);
                            break;
                        default:
                            break;
                    }
                }
                if (id.length() > 0) {
                    id.setLength(id.length() - 1);
                }
            } else if (keyObj != null && keyObj instanceof String) {
                id = id.append((String) keyObj);
            }
        }

        if (index == null || index.isEmpty()) {
            throw new DataException("Cannot infer index from sink record, please specify it in "
                    + ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG);
        }
        if (type == null || type.isEmpty()) {
            throw new DataException("Cannot infer type from sink record, please specify it in "
                    + ElasticsearchSinkConnectorConfig.TOPIC_TYPE_MAP_CONFIG);
        }
        if (id.length() == 0) {
            throw new DataException("Cannot extract id from sink record");
        }

        return new Key(index, type, id.toString());
    }

    private Struct extractData(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value == null) { // tombstone event
            return null;
        }

        if (!(value instanceof Struct)) {
            throw new DataException("SinkRecord value is not of struct type");
        }

        String field = dataField;
        if (field == null) {
            field = ElasticsearchSinkConnectorConstants.DATA_FIELD_DEFAULT;
        }
        if (field.isEmpty()) { // if data field is specified as empty, return the value itself as data
            return (Struct) value;
        } else {
            return ((Struct) value).getStruct(field);
        }
    }

    private Long extractDataVersion(SinkRecord sinkRecord) {
        // Version field name
        String field = versionField;
        if (field == null || field.isEmpty()) {
            field = ElasticsearchSinkConnectorConstants.VERSION_FIELD_DEFAULT;
        }
        // Read version from data
        Long version = null;
        Schema.Type versionType = null;
        Struct data = extractData(sinkRecord);
        if (data != null && data.schema().field(field) != null) {
            versionType = data.schema().field(field).schema().type();
        }
        if (versionType != null) {
            switch (versionType) {
                case INT8:
                    version = data.getInt8(field).longValue();
                    break;
                case INT16:
                    version = data.getInt16(field).longValue();
                    break;
                case INT32:
                    version = data.getInt32(field).longValue();
                    break;
                case INT64:
                    version = data.getInt64(field);
                    break;
                default:
                    logger.debug("Version type {} is not an integer type, version will be ignored", versionType);
            }
        }
        return version;
    }

    public void flush() {
        bulkProcessor.flush(flushTimeoutMs);
    }

    public void start() {
        bulkProcessor.start();
    }

    public void stop() {
        try {
            bulkProcessor.flush(flushTimeoutMs);
        } catch (Exception e) {
            logger.warn("Failed to flush during stop", e);
        }
        bulkProcessor.stop();
        bulkProcessor.awaitStop(flushTimeoutMs);
    }

    public void createIndicesForTopics(Set<String> assignedTopics) {
        for (String index : indicesForTopics(assignedTopics)) {
            if (!indexExists(index)) {
                CreateIndex createIndex = new CreateIndex.Builder(index).build();
                try {
                    JestResult result = client.execute(createIndex);
                    if (!result.isSucceeded()) {
                        String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
                        throw new ConnectException("Could not create index '" + index + "'" + msg);
                    }
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private boolean indexExists(String index) {
        io.searchbox.action.Action action = new IndicesExists.Builder(index).build();
        try {
            JestResult result = client.execute(action);
            return result.isSucceeded();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    private Set<String> indicesForTopics(Set<String> assignedTopics) {
        final Set<String> indices = new HashSet<>();
        for (String topic : assignedTopics) {
            final String index = topicToIndexMap.get(topic);
            if (index != null) {
                indices.add(index);
            } else {
                indices.add(indexFromTopic(topic));
            }
        }
        return indices;
    }

    private String indexFromTopic(String topic) {
        String[] topicLevels = topic.split("\\.");
        if (topicLevels.length > 2) {
            return topicLevels[topicLevels.length - 2];
        }
        return null;
    }

    private String typeFromTopic(String topic) {
        String[] topicLevels = topic.split("\\.");
        if (topicLevels.length > 2) {
            return topicLevels[topicLevels.length - 1];
        }
        return null;
    }
}
