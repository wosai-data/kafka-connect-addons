# Kafka ElasticSearch Sink Connector

This connector sinks confluent kafka messages into ElasticSearch. It works with Debezium's binlog source connector out of the box.

For each topic:

- If not specified, `Index` will be the second to last part of the topic delimited by `.`.
- If not specified, `Type` will be the last part of the topic delimited by `.`.

e.g. If the source kafka topic is `binlog.instance.db.table`, then `Index` will be `db` and `Type` will be `table`.

For each message:

- If key is a string, it will be used as the document key.
- If key is a struct, then each of its primitive type values will be transformed into a string. The strings are then concatenated into the document key, delimited by `:` or specified `id.delimiter`.
- By default, the document data is extracted from the field 'after' of the message value. Yet you can specify which field to extract with `data.field` option. Leave it empty, if the data is the message value itself.
- The connector will try to extract a version from each extracted document. The version will be used for indexing requests and help achieve exact once update. The version is critical, especially if the messages come from multiple kafka partitions, since updates could run of order during transmission. By default, the version will be extracted from the `version` field of the extracted document, yet you can specify it with the `version.field` option.
- A message with `null` value will be treated as a tombstone event (physical delete event). A delete document request will be sent to ElasticSearch.

## Prerequisites

```
Confluent Kafka: 3.1.2^
```

## Deploy

```
cd ${CONFLUENT_HOME}/share/java
tar zxf kafka-connect-binlog-elasticsearch-1.0-SNAPSHOT-dist.tar.gz
export CLASSPATH=$CLASSPATH:${CONFLUENT_HOME}/share/java/kafka-connect-binlog-elasticsearch-1.0-SNAPSHOT/*

```

## Configuration

### Connector

| Name | Type | Definition | Importance | Default Value |
| ---- | ---- | ---- | ---- | ---- |
| connection.url | List | List of Elasticsearch HTTP connection URLs, e.g. `"http://eshost1:9200,http://eshost2:9200"` | High | None |
| batch.size | Integer | The number of records to process as a batch when writing to Elasticsearch | Medium | 2000 |
| max.in.flight.requests | Integer | The maximum number of indexing requests that can be in-flight to Elasticsearch before blocking further requests | Medium | 5 |
| max.buffered.records | Integer | The maximum number of records each task will buffer before blocking acceptance of more records. This config can be used to limit the memory usage for each task | Low | 20000 |
| linger.ms | Long | Linger time in milliseconds for batching. Records that arrive in between request transmissions are batched into a single bulk indexing request, based on the `batch.size` configuration. Normally this only occurs under load when records arrive faster than they can be sent out. However it may be desirable to reduce the number of requests even under light load and benefit from bulk indexing. This setting helps accomplish that - when a pending batch is not full, rather than immediately sending it out the task will wait up to the given delay to allow other records to be added so that they can be batched into a single request | Low | 1 |
| flush.timeout.ms | Long | The timeout in milliseconds to use for periodic flushing, and when waiting for buffer space to be made available by completed requests as records are added. If this timeout is exceeded the task will fail | Low | 10000 |
| max.retries | Integer | The maximum number of retries that are allowed for failed indexing requests. If the retry attempts are exhausted the task will fail | Low | 5 |
| retry.backoff.ms | Long | How long to wait in milliseconds before attempting the first retry of a failed indexing request. Upon a failure, this connector may wait up to twice as long as the previous wait, up to the maximum number of retries. This avoids retrying in a tight loop under failure scenarios | Low | 100 |
|  |  |  |  |  |

### Conversion

| Name | Type | Definition | Importance | Default Value |
| ---- | ---- | ---- | ---- | ---- |
| topic.index.map | List | A map from Kafka topic name to the destination Elasticsearch index, represented as a list of `topic:index` pairs | Medium | "" |
| topic.type.map | List | A map from Kafka topic name to the destination Elasticsearch type, represented as a list of ``topic:type`` pairs | Medium | "" |
| id.delimiter | String | When a record's primary key contains multiple fields, delimiter will be used when concatenating key fields into a single document id | Medium | ":" |
| data.field | String | The field name from which record data will be extracted. When left empty, the message value itself will be extracted | High | "after" |
| version.field | String | The filed name from which record version will be extracted. The type of version field must be integer or long | Medium | "version" |
| drop.invalid.message | Boolean | Whether to drop kafka message when it cannot be converted to output message | Low | false |

## Sample

```
curl -XPOST -H "Content-Type: application/json" http://localhost:8083/connectors/ -d'
{
    "name" : "sink.elasticsearch.dmp_20171020",
    "config": {
        "connector.class" : "com.cloudest.connect.elasticsearch.ElasticsearchSinkConnector",
        "topics": "binlog.test1.dmp.user,binlog.test1.dmp.wechat_account",
        "tasks.max" : "1",
        "connection.url": "http://10.46.67.89:9200",
        "id.delimiter": "_",
        "drop.invalid.message": true
    }
}
'
```
