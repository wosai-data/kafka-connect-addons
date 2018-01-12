package com.cloudest.connect.elasticsearch;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class StructUtil {

    public static String evaluateFieldValue(Struct struct, String expression) {
        if (expression == null || expression.isEmpty()) {
            throw new IllegalArgumentException("expression is required.");
        }
        String[] args = expression.split("\\$");
        if (args.length != 1 && args.length != 3) {
            throw new IllegalArgumentException("expression format is invalid");
        }

        String fieldValue = extractStructValue(struct, args[0]);
        if (args.length == 3) {
            String[] valueSplits = fieldValue.split(String.valueOf("\\" + args[1]));
            int index = Integer.valueOf(args[2]);
            if (index >= valueSplits.length || index < 0) {
                throw new IllegalArgumentException("third parameter of expression is invalid");
            }
            return valueSplits[index];
        }
        if (fieldValue == null || fieldValue.isEmpty()) {
            throw new IllegalArgumentException("Invalid expression");
        }
        return fieldValue;
    }

    private static String extractStructValue(Struct struct, String fieldName) {
        Field field = struct.schema().field(fieldName);
        if (field == null) {
            return "";
        }
        Schema.Type fieldType = field.schema().type();
        if (!fieldType.isPrimitive()) {
            throw new IllegalArgumentException(
                    "Field " + fieldName + " is not of primitive type"
            );
        }
        String value;
        switch (fieldType) {
            case STRING:
                value = struct.getString(fieldName);
                break;
            case BYTES:
                value = new String(struct.getBytes(fieldName));
                break;
            case BOOLEAN:
                value = String.valueOf(struct.getBoolean(fieldName));
                break;
            case FLOAT32:
                value = String.valueOf(struct.getFloat32(fieldName));
                break;
            case FLOAT64:
                value = String.valueOf(struct.getFloat64(fieldName));
                break;
            case INT8:
                value = String.valueOf(struct.getInt8(fieldName));
                break;
            case INT16:
                value = String.valueOf(struct.getInt16(fieldName));
                break;
            case INT32:
                value = String.valueOf(struct.getInt32(fieldName));
                break;
            case INT64:
                value = String.valueOf(struct.getInt64(fieldName));
                break;
            default:
                value = "";
                break;
        }
        return value;
    }

    public static void main(String[] args) {
        Struct struct = new Struct(
                SchemaBuilder
                        .struct()
                        .field("account_id", SchemaBuilder.STRING_SCHEMA)
                        .build()
                        .schema()
        );
        struct.put("account_id", "123.456");

        System.out.println(evaluateFieldValue(struct, "account_id$$"));
        System.out.println(evaluateFieldValue(struct, "account_id$.$0"));
        System.out.println(evaluateFieldValue(struct, "account_id$.$1"));
        System.out.println(extractStructValue(struct, "account_id"));
    }

}
