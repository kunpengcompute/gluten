/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.HudiWriteJniWrapper
import org.apache.gluten.expression.OmniExpressionAdaptor.perBatchColumnOmniTypeIds
import org.apache.gluten.runtime.OmniRuntimes

import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Omni Hudi columnar write factory (aligned with OmniIcebergDataWriteFactory):
 * creates [[HudiWriteJniWrapper]] and [[OmniHudiColumnarBatchDataWriter]].
 *
 * @since 2026
 * @param schema    output schema for this write
 * @param directory Hudi base path (task output directory)
 * @param codec     parquet compression codec label from Hudi options
 * @param queryId   Spark query id used to build unique writer operation id
 */

case class OmniHudiDataWriteFactory(
    schema: StructType,
    directory: String,
    codec: String,
    queryId: String)
  extends ColumnarBatchDataWriterFactory
  with ColumnarStreamingDataWriterFactory {

  /** Batch write: creates a columnar [[DataWriter]] for the given partition/task. */
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    createWriter(partitionId, taskId, 0L)
  }

  /**
   * Batch or streaming write: `epochId` is appended to `queryId` so file names stay unique
   * across micro-batches.
   */
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    val operationId = queryId + "-" + epochId
    val jniWrapper = getJniWrapper(schema, directory, codec, partitionId, taskId, operationId)
    OmniHudiColumnarBatchDataWriter(jniWrapper)
  }

  private def getJniWrapper(
      localSchema: StructType,
      directory: String,
      codec: String,
      partitionId: Int,
      taskId: Long,
      operationId: String): HudiWriteJniWrapper = {
    val omniTypes = perBatchColumnOmniTypeIds(localSchema)
    val runtime = OmniRuntimes.contextInstance(
      BackendsApiManager.getBackendName, "HudiWrite#write")
    val jniWrapper = new HudiWriteJniWrapper(runtime)
    val params = new HudiWriteJniWrapper.HudiWriterInitParams(
      directory,
      codec,
      partitionId,
      taskId,
      operationId,
      SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) == "LEGACY")
    jniWrapper.init(localSchema, omniTypes, params)
    jniWrapper
  }
}
