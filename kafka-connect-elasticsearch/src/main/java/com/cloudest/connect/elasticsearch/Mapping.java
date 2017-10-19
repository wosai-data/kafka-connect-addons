package com.cloudest.connect.elasticsearch;

import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.mapping.GetMapping;

import java.io.IOException;

public class Mapping {
    /**
     * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
     */
    public static JsonObject getMapping(JestClient client, String index, String type)
            throws IOException {
        final JestResult result = client.execute(
                new GetMapping.Builder().addIndex(index).addType(type).build()
        );
        final JsonObject indexRoot = result.getJsonObject().getAsJsonObject(index);
        if (indexRoot == null) {
            return null;
        }
        final JsonObject mappingsJson = indexRoot.getAsJsonObject("mappings");
        if (mappingsJson == null) {
            return null;
        }
        return mappingsJson.getAsJsonObject(type);
    }
}
