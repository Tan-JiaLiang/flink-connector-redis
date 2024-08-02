package org.apache.flink.connector.redisv2.serde;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

public class RedisRowDataConverter {

    @FunctionalInterface
    public interface RowDataToStringConverter extends Serializable {
        String convert(RowData input);
    }

    @FunctionalInterface
    public interface StringToInternalConverter extends Serializable {
        Object convert(String input);
    }

    public static RowDataToStringConverter createNotNullExternalConverter(String name, LogicalType fieldType, int pos) {
        RowDataToStringConverter converter = createExternalConverter(fieldType, pos);
        return input -> {
            if (input.isNullAt(pos)) {
                throw new IllegalArgumentException(String.format("column %s can not be null.", name));
            } else {
                return converter.convert(input);
            }
        };
    }

    public static RowDataToStringConverter createNullableExternalConverter(LogicalType fieldType, int pos) {
        RowDataToStringConverter converter = createExternalConverter(fieldType, pos);
        return input -> {
            if (input.isNullAt(pos)) {
                return null;
            } else {
                return converter.convert(input);
            }
        };
    }

    public static StringToInternalConverter createNullableInternalConverter(LogicalType fieldType) {
        return createInternalConverter(fieldType);
    }

    private static RowDataToStringConverter createExternalConverter(LogicalType fieldType, int pos) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return input -> input.getString(pos).toString();
            case INTEGER:
                return input -> String.valueOf(input.getInt(pos));
            case BIGINT:
                return input -> String.valueOf(input.getLong(pos));
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    private static StringToInternalConverter createInternalConverter(LogicalType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryStringData::fromString;
            case INTEGER:
                return Integer::parseInt;
            case BIGINT:
                return Long::parseLong;
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }
}
