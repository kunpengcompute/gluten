/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;

/**
 * Streaming columnar DataWriter factory: creates writers with epochId for micro-batch/streaming
 * write. Companion to Spark's row-based {@link StreamingDataWriterFactory}.
 *
 * @since 2026
 */
public interface ColumnarStreamingDataWriterFactory extends Serializable {
    /**
     * Returns a data writer to do the actual writing work for the given epoch.
     *
     * @param partitionId unique id of the RDD partition that the returned writer will process
     * @param taskId      task id from {@link TaskContext#taskAttemptId()}
     * @param epochId     monotonically increasing id for streaming queries
     * @return the data writer for the given partition, task and epoch
     */
    DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId, long epochId);
}
