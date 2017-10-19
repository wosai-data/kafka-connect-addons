package com.cloudest.connect.elasticsearch.writer;

import com.cloudest.connect.elasticsearch.Key;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Index;

public class IndexableRecord extends SinkableRecord {
    public IndexableRecord(Key key, String payload, Long version) {
        super(key, payload, version);
    }

    @Override
    public BulkableAction toDocumentTargetedRequest() {
        Index.Builder builder = new Index.Builder(payload)
                .index(key.index)
                .type(key.type)
                .id(key.id);
        if (version != null) {
            builder.setParameter("version_type", "external").setParameter("version", version);
        }
        return builder.build();
    }
}
