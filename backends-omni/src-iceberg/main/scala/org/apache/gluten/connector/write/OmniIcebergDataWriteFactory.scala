/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.IcebergWriteJniWrapper
import org.apache.gluten.expression.OmniExpressionAdaptor.sparkTypeToOmniTypeWithComplex
import org.apache.gluten.runtime.OmniRuntimes

import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.{PartitionSpec, SortOrder}

/**
 * Omni Iceberg columnar write factory: creates IcebergWriteJniWrapper via init, then
 * OmniIcebergColumnarBatchDataWriter.
 */
case class OmniIcebergDataWriteFactory(
    schema: StructType,
    format: Int,
    directory: String,
    codec: String,
    partitionSpec: PartitionSpec,
    sortOrder: SortOrder,
    queryId: String)
  extends ColumnarBatchDataWriterFactory
  with ColumnarStreamingDataWriterFactory {

  /** Batch write entry: epochId is passed as 0. */
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    createWriter(partitionId, taskId, 0L)
  }

  /** Streaming/micro-batch write entry: uses queryId + epochId as operationId; creates writer via JNI wrapper. */
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    val operationId = queryId + "-" + epochId
    val jniWrapper = getJniWrapper(schema, format, directory, codec,
      partitionId, taskId, operationId, partitionSpec, sortOrder)
    OmniIcebergColumnarBatchDataWriter(jniWrapper, format, partitionSpec, sortOrder)
  }

  private def getJniWrapper(
      localSchema: StructType,
      format: Int,
      directory: String,
      codec: String,
      partitionId: Int,
      taskId: Long,
      operationId: String,
      partitionSpec: PartitionSpec,
      sortOrder: SortOrder): IcebergWriteJniWrapper = {
    val omniTypes = localSchema.fields
      .map(f => sparkTypeToOmniTypeWithComplex(f.dataType, f.metadata).getId.toValue())
      .toArray
    val runtime = OmniRuntimes.contextInstance(
      BackendsApiManager.getBackendName, "IcebergWrite#write")
    val jniWrapper = new IcebergWriteJniWrapper(runtime)
    val params = new IcebergWriteJniWrapper.IcebergWriterInitParams(
      format,
      directory,
      codec,
      partitionId,
      taskId,
      operationId,
      partitionSpec,
      sortOrder,
      SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) == "LEGACY")
    jniWrapper.init(localSchema, omniTypes, params)
    jniWrapper
  }
}
