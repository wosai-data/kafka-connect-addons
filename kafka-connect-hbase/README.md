### 安装
```
cd ${CONFLUENT_HOME}/share/java
tar zxf kafka-connect-hbase-1.0-SNAPSHOT-dist.tar.gz
export CLASSPATH=$CLASSPATH:${CONFLUENT_HOME}/share/java/kafka-connect-hbase-1.0-SNAPSHOT/*

```

### 配置参数
|名称 | 类型 | 解释 | 默认 |
|----|----|----|---|
|data.field | string | 写入HBase的数据从消息记录的字段中获取 | "" |
|zookeeper.quorum | string | 逗号分隔的ZK节点| 无 |
|hbase.rowkey.columns | string | 逗号分隔的rowkey字段| id  |
|hbase.rowkey.delimiter | string | 逗号分隔的rowkey字段分隔符 | , |
|hbase.column.family | string | 数据写到HBase CF名 | d |
|hbase.table.name | string | 数据写到HBase的表名 | topic名称 |
| | | | |

### 实例
```
curl -v -X POST -H "Content-Type: application/json" http://s1-kafka-001:8083/connectors/ -d '
{
 "name": "sink.hbase.transaction",
 "config": {
   "connector.class" : "com.cloudest.connect.hbase.HBaseSinkConnector",
   "tasks.max": "3",
   "topics": "stream.upay_transaction",
   "data.field": "after",
   "zookeeper.quorum": "s1-zk-001:2181,s1-zk-002:2181,s1-zk-003:2181",
   "hbase.rowkey.columns": "id",
   "hbase.rowkey.delimiter": "|",
   "hbase.column.family": "d",
   "hbase.table.name": "topic:upay_transaction"
  }
}
```
