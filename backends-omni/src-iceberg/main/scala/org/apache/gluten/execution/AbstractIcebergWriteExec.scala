/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.connector.write.{
  ColumnarBatchDataWriterFactory,
  ColumnarStreamingDataWriterFactory,
  OmniIcebergDataWriteFactory
}

import org.apache.spark.sql.types.StructType

import org.apache.iceberg.spark.source.IcebergWriteUtil

/**
 * Omni Iceberg write abstract base: createBatchWriterFactory and createStreamingWriterFactory
 * both return OmniIcebergDataWriteFactory; parameters are obtained from IcebergWriteUtil via
 * reflection (directory, format, codec, partition spec, etc.).
 */
abstract class AbstractIcebergWriteExec extends IcebergWriteExec {

  private def createOmniIcebergDataWriteFactory(schema: StructType): OmniIcebergDataWriteFactory = {
    val partitionSpec = IcebergWriteUtil.getPartitionSpec(write)
    OmniIcebergDataWriteFactory(
      schema,
      getFileFormat(IcebergWriteUtil.getFileFormat(write)),
      IcebergWriteUtil.getDirectory(write),
      getCodec,
      partitionSpec,
      IcebergWriteUtil.getSortOrder(write),
      IcebergWriteUtil.getQueryId(write)
    )
  }

  /** Batch write: returns Omni Iceberg columnar write factory. Partition spec is resolved on driver here. */
  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createOmniIcebergDataWriteFactory(schema)
  }

  /** Streaming/micro-batch write: same Omni Iceberg factory (epochId passed in createWriter(partId, taskId, epochId)). */
  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createOmniIcebergDataWriteFactory(schema)
  }
}
