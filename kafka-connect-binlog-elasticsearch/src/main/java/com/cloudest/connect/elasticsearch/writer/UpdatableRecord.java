package com.cloudest.connect.elasticsearch.writer;

import com.cloudest.connect.elasticsearch.Key;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Update;

public class UpdatableRecord extends SinkableRecord {

    public UpdatableRecord(Key key, String payload, Long version) {
        super(key, payload, version);
    }

    @Override
    public BulkableAction toDocumentTargetedRequest() {
        Update.Builder builder = new Update.Builder(payload)
                .index(key.index)
                .type(key.type)
                .id(key.id);
        return builder.build();
    }

}
