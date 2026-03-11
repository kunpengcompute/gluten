/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.metrics;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import java.util.ArrayList;

/**
 * Columnar write task metrics; returned by IcebergWriteJniWrapper.metrics().
 *
 * @since 2026
 */
public class BatchWriteMetrics {
    private final long numWrittenBytes;
    private final int numWrittenFiles;
    private final long writeIOTimeNs;
    private final long writeWallNs;

    /**
     * Constructs batch write metrics.
     *
     * @param numWrittenBytes total bytes written
     * @param numWrittenFiles number of files written
     * @param writeIOTimeNs   IO time in nanoseconds
     * @param writeWallNs     wall-clock time in nanoseconds
     */
    public BatchWriteMetrics(
            long numWrittenBytes, int numWrittenFiles, long writeIOTimeNs, long writeWallNs) {
        this.numWrittenBytes = numWrittenBytes;
        this.numWrittenFiles = numWrittenFiles;
        this.writeIOTimeNs = writeIOTimeNs;
        this.writeWallNs = writeWallNs;
    }

    /**
     * Converts to Spark CustomTaskMetric array for reporting.
     *
     * @return array of custom task metrics
     */
    public CustomTaskMetric[] toCustomTaskMetrics() {
        ArrayList<CustomTaskMetric> customTaskMetrics = new ArrayList<>();
        customTaskMetrics.add(customTaskMetric("numWrittenBytes", numWrittenBytes));
        customTaskMetrics.add(customTaskMetric("numWrittenFiles", numWrittenFiles));
        customTaskMetrics.add(customTaskMetric("writeIOTimeNs", writeIOTimeNs));
        customTaskMetrics.add(customTaskMetric("writeWallNs", writeWallNs));
        return customTaskMetrics.toArray(new CustomTaskMetric[0]);
    }

    private CustomTaskMetric customTaskMetric(String name, long value) {
        return new CustomTaskMetric() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long value() {
                return value;
            }
        };
    }
}
