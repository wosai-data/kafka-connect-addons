package com.cloudest.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FieldTimeBasedPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(FieldTimeBasedPartitioner.class);

    private String fieldName;
    // Duration of a partition in milliseconds.
    private long partitionDurationMs;
    private DateTimeFormatter formatter;
    protected List<FieldSchema> partitionFields = new ArrayList<>();
    private static String patternString = "'year'=Y{1,5}/('month'=M{1,5}/)?('day'=d{1,3}/)?('hour'=H{1,3}/)?('minute'=m{1,3}/)?";
    private static Pattern pattern = Pattern.compile(patternString);

    private static final DateFormat DF =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

    protected void init(String fieldName, long partitionDurationMs, String pathFormat, Locale locale,
                        DateTimeZone timeZone, boolean hiveIntegration) {
        this.fieldName = fieldName;
        this.partitionDurationMs = partitionDurationMs;
        this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
        addToPartitionFields(pathFormat, hiveIntegration);
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
        long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
        return timeZone.convertLocalToUTC(partitionedTime, false);
    }

    @Override
    public void configure(Map<String, Object> config) {
        String fieldName = (String)config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);
        if (fieldName == null || fieldName.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, fieldName, "field name cannot be empty.");
        }
        
        long partitionDurationMs = (long) config.get(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG);
        if (partitionDurationMs < 0) {
            throw new ConfigException(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG,
                                      partitionDurationMs, "Partition duration needs to be a positive.");
        }

        String pathFormat = (String) config.get(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG);
        if (pathFormat.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG,
                                      pathFormat, "Path format cannot be empty.");
        }

        String localeString = (String) config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG);
        if (localeString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.LOCALE_CONFIG,
                                      localeString, "Locale cannot be empty.");
        }
        String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
        if (timeZoneString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.TIMEZONE_CONFIG,
                                      timeZoneString, "Timezone cannot be empty.");
        }

        String hiveIntString = (String) config.get(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
        boolean hiveIntegration = hiveIntString != null && hiveIntString.toLowerCase().equals("true");

        Locale locale = new Locale(localeString);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
        init(fieldName, partitionDurationMs, pathFormat, locale, timeZone, hiveIntegration);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        long timestamp;
        Object value = sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();
        if (value instanceof Struct) {
          Struct struct = (Struct) value;
          Object timestampFieldValue = struct.get(fieldName);
          Type type = valueSchema.field(fieldName).schema().type();
          switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                timestamp = ((Number)timestampFieldValue).longValue();
                break;
            case STRING:
                try {
                    timestamp = Long.parseLong((String)timestampFieldValue);
                } catch (NumberFormatException e) {
                    try {  // add by terry timestamp format like '2017-06-02T11:06:55+08:00'
                        timestamp = DF.parse((String) timestampFieldValue).getTime();
                    } catch (ParseException e1) {
                        logger.error("timestamp {} format is invalid.",timestampFieldValue, e);
                        throw new RuntimeException(e1);
                    }
                }
                break;
            default:
              logger.error("Type {} is not supported as a timestamp partition field.", type.getName());
              throw new PartitionException("Error encoding partition.");
          }
        } else {
          logger.error("Value is not Struct type.");
          throw new PartitionException("Error encoding partition.");
        }

        DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
        return bucket.toString(formatter);
    }


    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + "/" + encodedPartition;
    }

    @Override
    public List<FieldSchema> partitionFields() {
        return partitionFields;
    }

    private boolean verifyDateTimeFormat(String pathFormat) {
        Matcher m = pattern.matcher(pathFormat);
        return m.matches();
    }

    private void addToPartitionFields(String pathFormat, boolean hiveIntegration) {
        if (hiveIntegration && !verifyDateTimeFormat(pathFormat)) {
            throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, pathFormat,
                                      "Path format doesn't meet the requirements for Hive integration, "
                                              + "which require prefixing each DateTime component with its name.");
        }
        for (String field: pathFormat.split("/")) {
            String[] parts = field.split("=");
            FieldSchema fieldSchema = new FieldSchema(parts[0].replace("'", ""), TypeInfoFactory.stringTypeInfo.toString(), "");
            partitionFields.add(fieldSchema);
        }
    }
}
