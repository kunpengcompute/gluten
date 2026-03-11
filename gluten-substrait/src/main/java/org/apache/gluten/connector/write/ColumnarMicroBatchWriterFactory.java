/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Micro-batch writer factory: delegates createWriter(partitionId, taskId) to the streaming factory's
 * createWriter(partitionId, taskId, epochId).
 *
 * @since 2026
 */
public class ColumnarMicroBatchWriterFactory implements ColumnarBatchDataWriterFactory {
    private final long epochId;
    private final ColumnarStreamingDataWriterFactory streamingWriterFactory;

    /**
     * Creates a micro-batch writer factory that delegates to the streaming factory with fixed epoch id.
     *
     * @param epochId                 epoch id for this micro-batch
     * @param streamingWriterFactory underlying streaming writer factory
     */
    public ColumnarMicroBatchWriterFactory(long epochId,
            ColumnarStreamingDataWriterFactory streamingWriterFactory) {
        this.epochId = epochId;
        this.streamingWriterFactory = streamingWriterFactory;
    }

    /**
     * Creates a writer for the given partition and task; delegates to streaming factory with epochId.
     *
     * @param partitionId partition id
     * @param taskId      task id
     * @return data writer for this micro-batch
     */
    @Override
    public DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId) {
        return streamingWriterFactory.createWriter(partitionId, taskId, epochId);
    }
}
