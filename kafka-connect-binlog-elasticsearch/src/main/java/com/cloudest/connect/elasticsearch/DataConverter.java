package com.cloudest.connect.elasticsearch;

import com.cloudest.connect.elasticsearch.writer.DeletableRecord;
import com.cloudest.connect.elasticsearch.writer.IndexableRecord;
import com.cloudest.connect.elasticsearch.writer.SinkableRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class DataConverter {
    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        // TODO test true and false
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public SinkableRecord convertToSinkableRecord(
            Struct struct,
            SinkableRecord.Operation operation,
            Key key,
            Long version
    ) {
        SinkableRecord record = null;
        if (operation == SinkableRecord.Operation.INDEX) {
            byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(null, struct.schema(), struct);
            final String payload = new String(rawJsonPayload, StandardCharsets.UTF_8);
            record = new IndexableRecord(key, payload, version);
        } else if (operation == SinkableRecord.Operation.DELETE) {
            record = new DeletableRecord(key, null, version);
        }
        return record;
    }
}
