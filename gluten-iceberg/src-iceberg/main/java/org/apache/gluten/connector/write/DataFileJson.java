/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * JSON representation of DataFile for deserializing JNI-returned strings on commit.
 *
 * @since 2026
 */
public class DataFileJson {
    /** File path. */
    @JsonProperty
    private String path;

    /** Iceberg metrics wrapper. */
    private MetricsWrapper metrics;

    /** Split offsets. */
    private List<Long> splitOffsets;

    /** Content type. */
    private String content;

    /** Referenced data file. */
    private String referencedDataFile;

    /** Partition spec id. */
    private Integer partitionSpecJson;

    /** Partition data JSON string. */
    private String partitionDataJson;

    /** File size in bytes. */
    private long fileSizeInBytes = -1L;

    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @JsonProperty("metrics")
    public MetricsWrapper getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsWrapper metrics) {
        this.metrics = metrics;
    }

    @JsonProperty("splitOffsets")
    public List<Long> getSplitOffsets() {
        return splitOffsets;
    }

    public void setSplitOffsets(List<Long> splitOffsets) {
        this.splitOffsets = splitOffsets;
    }

    @JsonProperty("content")
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @JsonProperty("referencedDataFile")
    public String getReferencedDataFile() {
        return referencedDataFile;
    }

    public void setReferencedDataFile(String referencedDataFile) {
        this.referencedDataFile = referencedDataFile;
    }

    @JsonProperty("partitionSpecJson")
    public Integer getPartitionSpecJson() {
        return partitionSpecJson;
    }

    public void setPartitionSpecJson(Integer partitionSpecJson) {
        this.partitionSpecJson = partitionSpecJson;
    }

    @JsonProperty("partitionDataJson")
    public String getPartitionDataJson() {
        return partitionDataJson;
    }

    public void setPartitionDataJson(String partitionDataJson) {
        this.partitionDataJson = partitionDataJson;
    }

    @JsonProperty("fileSizeInBytes")
    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
    }
}
