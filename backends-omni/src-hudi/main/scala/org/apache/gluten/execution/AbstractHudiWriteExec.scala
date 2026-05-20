/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.gluten.connector.write.{
  ColumnarBatchDataWriterFactory,
  ColumnarStreamingDataWriterFactory,
  OmniHudiDataWriteFactory
}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import org.apache.gluten.backendsapi.omni.HudiWriteUtil

/**
 * Omni Hudi write abstract base (aligned with AbstractIcebergWriteExec):
 * createBatchWriterFactory and createStreamingWriterFactory return OmniHudiDataWriteFactory;
 * parameters from HudiWriteUtil (directory, codec, queryId).
 *
 * @since 2026
 */

abstract class AbstractHudiWriteExec extends HudiWriteExec {

  override protected def run(): Seq[InternalRow] = {
    super.run()
  }

  private def createOmniHudiDataWriteFactory(schema: StructType): OmniHudiDataWriteFactory = {
    OmniHudiDataWriteFactory(
      schema,
      HudiWriteUtil.getDirectory(write),
      HudiWriteUtil.getCodec(write),
      HudiWriteUtil.getFileFormat(write),
      HudiWriteUtil.getQueryId(write)
    )
  }

  /** DSv2 batch path: factory that emits [[OmniHudiColumnarBatchDataWriter]] instances. */
  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createOmniHudiDataWriteFactory(schema)
  }

  /** DSv2 streaming path: same factory type supports micro-batch epoch ids. */
  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createOmniHudiDataWriteFactory(schema)
  }
}
