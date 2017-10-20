package com.cloudest.connect.elasticsearch.writer;

import com.cloudest.connect.elasticsearch.Key;
import io.searchbox.action.BulkableAction;

public abstract class SinkableRecord {
    public enum Operation {
        INDEX,
        DELETE
    }

    public final Key key;
    public final String payload;
    public final Long version;

    public SinkableRecord(Key key, String payload, Long version) {
        this.key = key;
        this.version = version;
        this.payload = payload;
    }

    public abstract BulkableAction toDocumentTargetedRequest();
}
