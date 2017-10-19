package com.cloudest.connect.elasticsearch;

import com.cloudest.connect.elasticsearch.bulk.BulkClient;
import com.cloudest.connect.elasticsearch.bulk.BulkResponse;
import com.cloudest.connect.elasticsearch.writer.SinkableRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;

public class BulkIndexingClient implements BulkClient<SinkableRecord, Bulk> {

    private static final Logger logger = LoggerFactory.getLogger(BulkIndexingClient.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JestClient client;

    public BulkIndexingClient(JestClient client) {
        this.client = client;
    }

    @Override
    public Bulk bulkRequest(List<SinkableRecord> batch) {
        final Bulk.Builder builder = new Bulk.Builder();
        for (SinkableRecord record : batch) {
            builder.addAction(record.toDocumentTargetedRequest());
        }
        return builder.build();
    }

    @Override
    public BulkResponse execute(Bulk bulk) throws IOException {
        final BulkResult result = client.execute(bulk);

        if (result.isSucceeded()) {
            return BulkResponse.success();
        }

        boolean retriable = true;

        final List<Key> versionConflicts = new ArrayList<>();
        final List<String> errors = new ArrayList<>();

        for (BulkResult.BulkResultItem item : result.getItems()) {
            if (item.error != null) {
                final ObjectNode parsedError = (ObjectNode) OBJECT_MAPPER.readTree(item.error);
                final String errorType = parsedError.get("type").asText("");
                if ("version_conflict_engine_exception".equals(errorType)) {
                    versionConflicts.add(new Key(item.index, item.type, item.id));
                } else if ("mapper_parse_exception".equals(errorType)) {
                    retriable = false;
                    errors.add(item.error);
                } else {
                    errors.add(item.error);
                }
            }
        }

        if (!versionConflicts.isEmpty()) {
            logger.debug("Ignoring version conflicts for items: {}", versionConflicts);
            if (errors.isEmpty()) {
                // The only errors were version conflicts
                return BulkResponse.success();
            }
        }

        final String errorInfo = errors.isEmpty() ? result.getErrorMessage() : errors.toString();

        return BulkResponse.failure(retriable, errorInfo);
    }
}
