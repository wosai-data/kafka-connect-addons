package com.cloudest.connect.elasticsearch.writer;

import com.cloudest.connect.elasticsearch.Key;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;

public class DeletableRecord extends SinkableRecord {
    public DeletableRecord(Key key, String payload, Long version) {
        super(key, payload, version);
    }

    @Override
    public BulkableAction toDocumentTargetedRequest() {
        Delete.Builder builder = new Delete.Builder(key.id)
                .index(key.index)
                .type(key.type)
                .id(key.id);
        return builder.build();
    }
}
