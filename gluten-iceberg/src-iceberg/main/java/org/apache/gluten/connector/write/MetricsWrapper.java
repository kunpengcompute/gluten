/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.iceberg.Metrics;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * JSON wrapper for Iceberg Metrics; used for DataFileJson serialization/deserialization.
 *
 * @since 2026
 */
public class MetricsWrapper {
    private final Metrics metrics;

    /**
     * Constructor for Jackson deserialization.
     *
     * @param recordCount       record count
     * @param columnSizes       column sizes map
     * @param valueCounts       value counts map
     * @param nullValueCounts   null value counts map
     * @param nanValueCounts    nan value counts map
     * @param lowerBounds       lower bounds map
     * @param upperBounds       upper bounds map
     */
    @JsonCreator
    public MetricsWrapper(
            @JsonProperty("recordCount") Long recordCount,
            @JsonProperty("columnSizes") Map<Integer, Long> columnSizes,
            @JsonProperty("valueCounts") Map<Integer, Long> valueCounts,
            @JsonProperty("nullValueCounts") Map<Integer, Long> nullValueCounts,
            @JsonProperty("nanValueCounts") Map<Integer, Long> nanValueCounts,
            @JsonProperty("lowerBounds") Map<Integer, ByteBuffer> lowerBounds,
            @JsonProperty("upperBounds") Map<Integer, ByteBuffer> upperBounds) {
        this(new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts,
                lowerBounds, upperBounds));
    }

    /**
     * Wraps an Iceberg Metrics instance.
     *
     * @param metrics Iceberg Metrics, not null
     */
    public MetricsWrapper(Metrics metrics) {
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    /**
     * Returns the wrapped Iceberg Metrics.
     *
     * @return the metrics instance
     */
    public Metrics metrics() {
        return metrics;
    }

    /**
     * Returns record count for JSON serialization.
     *
     * @return record count
     */
    @JsonProperty("recordCount")
    public Long getRecordCount() {
        return metrics.recordCount();
    }

    /**
     * Returns column sizes map for JSON serialization.
     *
     * @return column sizes map
     */
    @JsonProperty("columnSizes")
    public Map<Integer, Long> getColumnSizes() {
        return metrics.columnSizes();
    }

    /**
     * Returns value counts map for JSON serialization.
     *
     * @return value counts map
     */
    @JsonProperty("valueCounts")
    public Map<Integer, Long> getValueCounts() {
        return metrics.valueCounts();
    }

    /**
     * Returns null value counts map for JSON serialization.
     *
     * @return null value counts map
     */
    @JsonProperty("nullValueCounts")
    public Map<Integer, Long> getNullValueCounts() {
        return metrics.nullValueCounts();
    }

    /**
     * Returns nan value counts map for JSON serialization.
     *
     * @return nan value counts map
     */
    @JsonProperty("nanValueCounts")
    public Map<Integer, Long> getNanValueCounts() {
        return metrics.nanValueCounts();
    }

    /**
     * Returns lower bounds map for JSON serialization.
     *
     * @return lower bounds map
     */
    @JsonProperty("lowerBounds")
    public Map<Integer, ByteBuffer> getLowerBounds() {
        return metrics.lowerBounds();
    }

    /**
     * Returns upper bounds map for JSON serialization.
     *
     * @return upper bounds map
     */
    @JsonProperty("upperBounds")
    public Map<Integer, ByteBuffer> getUpperBounds() {
        return metrics.upperBounds();
    }
}
