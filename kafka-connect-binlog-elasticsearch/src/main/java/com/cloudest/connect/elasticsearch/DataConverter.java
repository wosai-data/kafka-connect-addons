package com.cloudest.connect.elasticsearch;

import com.cloudest.connect.elasticsearch.writer.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


public class DataConverter {

    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        // TODO test true and false
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public SinkableRecord convertToSinkableRecord(
            Struct struct,
            SinkableRecord.Action action,
            Key key,
            Long version,
            Map<String, Object> conversionConfigs
    ) {
        SinkableRecord record = null;
        switch (action) {
            case INDEX: {
                Struct convertedStruct = convertStruct(struct, conversionConfigs);
                byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(null, convertedStruct.schema(), convertedStruct);
                final String payload = new String(rawJsonPayload, StandardCharsets.UTF_8);
                record = new IndexableRecord(key, payload, version);
                break;
            }
            case UPSERT: {
                Struct convertedStruct = convertStruct(struct, conversionConfigs);
                Struct upsertStruct = wrapStructForUpsert(convertedStruct, conversionConfigs);
                byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(null, upsertStruct.schema(), upsertStruct);
                final String payload = new String(rawJsonPayload, StandardCharsets.UTF_8);
                record = new UpdatableRecord(key, payload, version);
                break;
            }
            case DELETE: {
                String updateOnDeleteScriptId = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.UPDATE_ON_DELETE_CONFIG);
                if (updateOnDeleteScriptId != null && !updateOnDeleteScriptId.isEmpty()) {
                    Struct updateStruct = structForUpdateOnDelete(conversionConfigs);
                    byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(null, updateStruct.schema(), updateStruct);
                    final String payload = new String(rawJsonPayload, StandardCharsets.UTF_8);
                    record = new UpdatableRecord(key, payload, version);
                } else {
                    record = new DeletableRecord(key, null, version);
                }
                break;
            }
            default:
        }
        return record;
    }

    private Struct convertStruct(Struct struct, Map<String, Object> conversionConfigs) {
        Struct bytesToStringStruct = convertBytesToString(struct, conversionConfigs);
        return convertStringToList(bytesToStringStruct, conversionConfigs);
    }

    @SuppressWarnings("unchecked")
    private Struct convertStringToList(Struct struct, Map<String, Object> conversionConfigs) {
        List<String> stringListFields = (List<String>) conversionConfigs.get(
                ElasticsearchSinkConnectorConfig.STRING_LIST_FIELDS_CONFIG
        );
        List<String> integerListFields = (List<String>) conversionConfigs.get(
                ElasticsearchSinkConnectorConfig.INTEGER_LIST_FIELDS_CONFIG
        );

        if ((stringListFields == null || stringListFields.isEmpty())
                && (integerListFields == null || integerListFields.isEmpty())) {
            return struct;
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Field field : struct.schema().fields()) {
            String fieldName = field.name();
            if (stringListFields != null && stringListFields.contains(fieldName)) {
                Schema newValueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
                schemaBuilder.field(fieldName, newValueSchema);
            } else if (integerListFields != null && integerListFields.contains(fieldName)) {
                Schema newValueSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
                schemaBuilder.field(fieldName, newValueSchema);
            } else {
                schemaBuilder.field(fieldName, field.schema());
            }
        }
        Schema newSchema = schemaBuilder.build();
        Struct newStruct = new Struct(newSchema);
        for (Field field : newSchema.fields()) {
            final String fieldName = field.name();
            if (stringListFields != null && stringListFields.contains(fieldName)) {
                String fieldValue = struct.getString(fieldName);
                if (fieldValue == null || fieldValue.isEmpty()) {
                    newStruct.put(fieldName, new ArrayList<>());
                } else {
                    List<String> newValue = Arrays
                            .stream(fieldValue.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
                    newStruct.put(fieldName, newValue);
                }
            } else if (integerListFields != null && integerListFields.contains(fieldName)) {
                String fieldValue = struct.getString(fieldName);
                if (fieldValue == null || fieldValue.isEmpty()) {
                    newStruct.put(fieldName, new ArrayList<>());
                } else {
                    List<Integer> newValue = Arrays
                            .stream(fieldValue.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .map(Integer::valueOf)
                            .collect(Collectors.toList());
                    newStruct.put(fieldName, newValue);
                }
            } else {
                newStruct.put(fieldName, struct.get(fieldName));
            }
        }
        return newStruct;
    }

    @SuppressWarnings("unchecked")
    private Struct convertBytesToString(Struct struct, Map<String, Object> conversionConfigs) {
        List<String> bytesToStringFields = (List<String>) conversionConfigs.get(
                ElasticsearchSinkConnectorConfig.BYTES_TO_STRING_FIELDS_CONFIG
        );

        if (bytesToStringFields == null || bytesToStringFields.isEmpty()) {
            return struct;
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Field field : struct.schema().fields()) {
            String fieldName = field.name();
            if (bytesToStringFields.contains(fieldName)) {
                Schema newValueSchema = SchemaBuilder.string().optional().build();
                schemaBuilder.field(fieldName, newValueSchema);
            } else {
                schemaBuilder.field(fieldName, field.schema());
            }
        }
        Schema newSchema = schemaBuilder.build();
        Struct newStruct = new Struct(newSchema);
        for (Field field : newSchema.fields()) {
            final String fieldName = field.name();
            if (bytesToStringFields.contains(fieldName)) {
                byte[] fieldValue = struct.getBytes(fieldName);
                if (fieldValue != null && fieldValue.length > 0) {
                    newStruct.put(fieldName, new String(fieldValue));
                }
            } else {
                newStruct.put(fieldName, struct.get(fieldName));
            }
        }
        return newStruct;
    }

    private Struct wrapStructForUpsert(Struct struct, Map<String, Object> conversionConfigs) {
        // Make document struct
        String sinkToField = null;
        String sinkToFieldKeyExpression = null;
        String updateScriptId = null;
        int retry = ElasticsearchSinkConnectorConstants.VERSION_CONFLICT_RETRIES_DEFAULT;
        if (conversionConfigs != null) {
            sinkToField = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_CONFIG);
            sinkToFieldKeyExpression = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_ARG_CONFIG);
            updateScriptId = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.UPDATE_SCRIPT_ID_CONFIG);
            String retryExp = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.VERSION_CONFLICT_RETRIES_CONFIG);
            if (retryExp != null && !retryExp.isEmpty()) {
                retry = Integer.valueOf(retryExp);
            }
        }
        List<String> structPaths = makeFullyQualifiedPaths(sinkToField, sinkToFieldKeyExpression, struct);
        Struct lastLevel = struct;
        for (int i = structPaths.size() - 1; i >= 0; i--) {
            String path = structPaths.get(i);
            Schema outerLevelSchema = SchemaBuilder
                    .struct()
                    .field(path, lastLevel.schema())
                    .build();
            Struct outerLevel = new Struct(outerLevelSchema);
            outerLevel.put(path, lastLevel);
            lastLevel = outerLevel;
        }

        // Wrap for upsert
        Struct wrapped;
        if (updateScriptId != null && !updateScriptId.isEmpty()) {
            Schema paramSchema = SchemaBuilder
                    .struct()
                    .field("param", lastLevel.schema())
                    .build();
            Struct params = new Struct(paramSchema);
            params.put("param", lastLevel);
            Schema scriptSchema = SchemaBuilder
                    .struct()
                    .field("stored", Schema.STRING_SCHEMA)
                    .field("params", paramSchema)
                    .build();
            Struct script = new Struct(scriptSchema);
            script.put("stored", updateScriptId);
            script.put("params", params);

            Schema upsertSchema = SchemaBuilder.struct().build();
            Schema wrapSchema = SchemaBuilder
                    .struct()
                    .field("script", scriptSchema)
                    .field("scripted_upsert", Schema.BOOLEAN_SCHEMA)
                    .field("upsert", upsertSchema)
                    .field("retry_on_conflict", Schema.INT32_SCHEMA)
                    .build();
            wrapped = new Struct(wrapSchema);
            wrapped.put("script", script);
            wrapped.put("scripted_upsert", true);
            wrapped.put("upsert", new Struct(upsertSchema));
            wrapped.put("retry_on_conflict", retry);
        } else {
            Schema wrapSchema = SchemaBuilder
                    .struct()
                    .field("doc_as_upsert", Schema.BOOLEAN_SCHEMA)
                    .field("retry_on_conflict", Schema.INT32_SCHEMA)
                    .field("doc", lastLevel.schema())
                    .build();
            wrapped = new Struct(wrapSchema);
            wrapped.put("doc_as_upsert", true);
            wrapped.put("retry_on_conflict", retry);
            wrapped.put("doc", lastLevel);
        }
        return wrapped;
    }

    private Struct structForUpdateOnDelete(Map<String, Object> conversionConfigs) {
        int retry = ElasticsearchSinkConnectorConstants.VERSION_CONFLICT_RETRIES_DEFAULT;
        String retryExp = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.VERSION_CONFLICT_RETRIES_CONFIG);
        if (retryExp != null && !retryExp.isEmpty()) {
            retry = Integer.valueOf(retryExp);
        }
        String updateScriptId = (String) conversionConfigs.get(ElasticsearchSinkConnectorConfig.UPDATE_ON_DELETE_CONFIG);
        Schema scriptSchema = SchemaBuilder
                .struct()
                .field("stored", Schema.STRING_SCHEMA)
                .build();
        Struct script = new Struct(scriptSchema);
        script.put("stored", updateScriptId);

        Schema wrapSchema = SchemaBuilder
                .struct()
                .field("script", scriptSchema)
                .field("retry_on_conflict", Schema.INT32_SCHEMA)
                .build();
        Struct wrapped = new Struct(wrapSchema);
        wrapped.put("script", script);
        wrapped.put("retry_on_conflict", retry);
        return wrapped;
    }

    private List<String> makeFullyQualifiedPaths(String sinkToField, String sinkToFieldKeyExpression, Struct struct) {
        List<String> paths = new ArrayList<>();
        if (sinkToField != null && !sinkToField.isEmpty()) {
            String[] rawPaths = sinkToField.split("\\.");
            for (String rawPath : rawPaths) {
                if ("{}".equals(rawPath)) {
                    paths.add(StructUtil.evaluateFieldValue(struct, sinkToFieldKeyExpression));
                } else if (!rawPath.isEmpty()) {
                    paths.add(rawPath);
                }
            }
        }
        return paths;
    }

    public static void main(String[] args) {
        Map<String, Object> conversionConfigs = new HashMap<>();
//        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_CONFIG, "account.wechat.{}");
//        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_ARG_CONFIG, "account_id$.$0");
//        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_ARG_CONFIG, "account_id$.$1");
//        conversionConfigs.put(ElasticsearchSinkConnectorConfig.SINK_FIELD_PATHS_ARG_CONFIG, "account_id$$");
        conversionConfigs.put(ElasticsearchSinkConnectorConfig.UPDATE_SCRIPT_ID_CONFIG, "abc");
        List<String> fd = new ArrayList<>();
        fd.add("account_id");
        conversionConfigs.put(ElasticsearchSinkConnectorConfig.STRING_LIST_FIELDS_CONFIG, fd);
        Struct struct = new Struct(
                SchemaBuilder
                        .struct()
                        .field("account_id", SchemaBuilder.STRING_SCHEMA)
                        .field("app_id", SchemaBuilder.STRING_SCHEMA)
                        .field("extra", SchemaBuilder.STRING_SCHEMA)
                        .field("test", SchemaBuilder.string().optional().build())
                        .build()
                        .schema()
        );
        struct.put("account_id", "123456");
        struct.put("app_id", "123.456");
        struct.put("extra", "{\"what\":\"wow\"}");

        DataConverter converter = new DataConverter();
        Struct convertedStruct = converter.convertStringToList(struct, conversionConfigs);
        Struct upsertStruct = converter.wrapStructForUpsert(convertedStruct, conversionConfigs);
        byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(null, upsertStruct.schema(), upsertStruct);
        final String payload = new String(rawJsonPayload, StandardCharsets.UTF_8);
        System.out.println(payload);
        SchemaAndValue sv = JSON_CONVERTER.toConnectData("topic", "{\"app_id\":\"123\"}".getBytes());
        System.out.println(sv.schema());
        System.out.println(sv);
    }

}
