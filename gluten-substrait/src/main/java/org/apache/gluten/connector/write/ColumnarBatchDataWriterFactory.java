/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;

/**
 * Columnar DataWriter factory: creates and initializes the actual columnar data writer at executor
 * side. Companion to Spark's row-based {@link DataWriterFactory}.
 *
 * @since 2026
 */
@Evolving
public interface ColumnarBatchDataWriterFactory extends Serializable {
    /**
     * Returns a data writer to do the actual writing work.
     *
     * @param partitionId unique id of the RDD partition that the returned writer will process
     * @param taskId      task id from {@link TaskContext#taskAttemptId()}
     * @return the data writer for the given partition and task
     */
    DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId);
}
