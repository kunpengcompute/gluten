/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON representation of one written file for Hudi commit (path, size, record count).
 * Used by HudiWriteJniWrapper.commit() and then converted to Hudi's WriterCommitMessage.
 *
 * @since 2026
 */

public class HudiFileInfoJson {
    @JsonProperty("path")
    private String path;

    @JsonProperty("fileSizeInBytes")
    private long fileSizeInBytes;

    @JsonProperty("recordCount")
    private long recordCount;

    /**
     * Absolute or FS path of the written Parquet file.
     *
     * @return path
     */
    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    /**
     * Sets the output file path (used when serializing from native write).
     *
     * @param path path
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * File length in bytes after the writer closed the file.
     *
     * @return fileSize
     */
    @JsonProperty("fileSizeInBytes")
    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
    }

    /**
     * Number of rows written to this file in the current commit.
     *
     * @return fileSize
     */
    @JsonProperty("recordCount")
    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }
}
