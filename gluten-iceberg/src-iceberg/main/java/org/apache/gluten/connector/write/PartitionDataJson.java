/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Partition data JSON parsing; used for DataFileJson.partitionDataJson -> StructLike.
 *
 * @since 2026
 */
public class PartitionDataJson implements StructLike {
    private static final String PARTITION_VALUES_FIELD = "partitionValues";
    private static final JsonFactory FACTORY = new JsonFactory();
    private static final ObjectMapper MAPPER =
            new ObjectMapper(FACTORY).configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private final List<Object> partitionValues;

    /**
     * Creates a partition data holder from the given value list.
     *
     * @param partitionValues partition value list, not null
     */
    public PartitionDataJson(List<Object> partitionValues) {
        this.partitionValues = new ArrayList<>(requireNonNull(partitionValues, "partitionValues is null"));
    }

    @Override
    public int size() {
        return partitionValues.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Object value = partitionValues.get(pos);
        if (javaClass == ByteBuffer.class && value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
        }
        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        }
        throw new IllegalArgumentException(
                format("Wrong class [%s] for object class [%s]", javaClass.getName(), value.getClass().getName()));
    }

    @Override
    public <T> void set(int pos, T value) {
        partitionValues.set(pos, value);
    }

    /**
     * Parses partition data from JSON string.
     *
     * @param partitionDataAsJson JSON string or null
     * @param partitionSpec       partition spec
     * @return parsed instance or empty if null/invalid
     */
    public static Optional<PartitionDataJson> fromJson(String partitionDataAsJson, PartitionSpec partitionSpec) {
        if (partitionDataAsJson == null) {
            return Optional.empty();
        }
        JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(partitionDataAsJson);
        } catch (IOException e) {
            throw new UncheckedIOException("Conversion from JSON failed for PartitionData: " + partitionDataAsJson, e);
        }
        if (jsonNode.isNull()) {
            return Optional.empty();
        }
        JsonNode partitionValuesNode = jsonNode.get(PARTITION_VALUES_FIELD);
        List<Object> objects = new ArrayList<>(partitionSpec.fields().size());
        int index = 0;
        for (JsonNode partitionValue : partitionValuesNode) {
            Type partionType = partitionSpec.partitionType().fields().get(index).type();
            objects.add(getValue(partitionValue, partionType).orElse(null));
            index++;
        }
        return Optional.of(new PartitionDataJson(objects));
    }

    /**
     * Serializes partition value list to JSON string for DataFileJson.partitionDataJson.
     *
     * @param partitionValues partition value list or null
     * @return JSON string or empty if null/empty list
     */
    public static Optional<String> toJson(List<Object> partitionValues) {
        if (partitionValues == null || partitionValues.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> map = new HashMap<>();
        map.put(PARTITION_VALUES_FIELD, partitionValues);
        try {
            return Optional.of(MAPPER.writeValueAsString(map));
        } catch (IOException e) {
            throw new UncheckedIOException("Serialize partition values failed", e);
        }
    }

    /**
     * Converts partitionDataJson to Hive-style path segment (e.g. "vendor_id=1" or
     * "vendor_id=1/dt=2024-01-01"), aligned with native Spark/Iceberg.
     *
     * @param partitionDataAsJson partition data JSON or null
     * @param partitionSpec       partition spec
     * @return path segment string, or empty if not partitioned
     */
    public static String toPathSegment(String partitionDataAsJson, PartitionSpec partitionSpec) {
        if (partitionDataAsJson == null || partitionSpec.fields().isEmpty()) {
            return "";
        }
        return fromJson(partitionDataAsJson, partitionSpec).map(pd -> buildPathSegment(pd, partitionSpec)).orElse("");
    }

    private static String buildPathSegment(PartitionDataJson pd, PartitionSpec partitionSpec) {
        int n = partitionSpec.fields().size();
        List<Types.NestedField> partitionTypeFields = null;
        if (partitionSpec.partitionType() instanceof Types.StructType) {
            partitionTypeFields = ((Types.StructType) partitionSpec.partitionType()).fields();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append("/");
            }
            String name = (partitionTypeFields != null && i < partitionTypeFields.size())
                    ? partitionTypeFields.get(i).name()
                    : partitionSpec.fields().get(i).name();
            Object val = pd.get(i, Object.class);
            sb.append(name).append("=").append(valueToPathString(val));
        }
        return sb.toString();
    }

    private static String valueToPathString(Object val) {
        if (val == null) {
            return "null";
        }
        return val.toString();
    }

    /**
     * Gets partition value from JSON node by type.
     *
     * @param partitionValue JSON node
     * @param type           partition column type
     * @return Java value or empty if null node
     */
    public static Optional<Object> getValue(JsonNode partitionValue, Type type) {
        if (partitionValue.isNull()) {
            return Optional.empty();
        }
        switch (type.typeId()) {
            case BOOLEAN:
                return Optional.of(partitionValue.asBoolean());
            case INTEGER:
            case DATE:
                return Optional.of(partitionValue.asInt());
            case LONG:
            case TIMESTAMP:
            case TIME:
                return Optional.of(partitionValue.asLong());
            case FLOAT:
                return Optional.of(parseFloatValue(partitionValue));
            case DOUBLE:
                return Optional.of(parseDoubleValue(partitionValue));
            case STRING:
                return Optional.of(partitionValue.asText());
            case FIXED:
            case BINARY:
                try {
                    return Optional.of(partitionValue.binaryValue());
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed during JSON conversion of " + partitionValue, e);
                }
            case DECIMAL:
                if (type instanceof Types.DecimalType) {
                    Types.DecimalType decimalType = (Types.DecimalType) type;
                    return Optional.of(partitionValue.decimalValue().setScale(decimalType.scale()));
                }
                break;
            default:
                break;
        }
        throw new UnsupportedOperationException("Type not supported as partition column: " + type);
    }

    private static float parseFloatValue(JsonNode partitionValue) {
        if (partitionValue.asText().equalsIgnoreCase("NaN")) {
            return Float.NaN;
        }
        if (partitionValue.asText().equalsIgnoreCase("Infinity")) {
            return Float.POSITIVE_INFINITY;
        }
        if (partitionValue.asText().equalsIgnoreCase("-Infinity")) {
            return Float.NEGATIVE_INFINITY;
        }
        return partitionValue.floatValue();
    }

    private static double parseDoubleValue(JsonNode partitionValue) {
        if (partitionValue.asText().equalsIgnoreCase("NaN")) {
            return Double.NaN;
        }
        if (partitionValue.asText().equalsIgnoreCase("Infinity")) {
            return Double.POSITIVE_INFINITY;
        }
        if (partitionValue.asText().equalsIgnoreCase("-Infinity")) {
            return Double.NEGATIVE_INFINITY;
        }
        return partitionValue.doubleValue();
    }
}
