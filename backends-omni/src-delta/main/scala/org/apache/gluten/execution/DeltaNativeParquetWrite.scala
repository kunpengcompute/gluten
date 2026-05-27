/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.SparkSession

/**
 * Controls the session-local switch for Omni native Parquet output inside Delta writes.
 *
 * Delta keeps its transaction semantics, while this marker lets the underlying FileFormatWriter
 * choose Omni's Parquet writer for the data files generated during INSERT/APPEND.
 */
object DeltaNativeParquetWrite {
  val NativeApplicableKey = "isNativeApplicable"
  val NativeFormatKey = "nativeFormat"
  val NativeDeltaWriteKey = "gluten.omni.delta.native.parquet.write"

  def enable(session: SparkSession): Unit = {
    session.sparkContext.setLocalProperty(NativeApplicableKey, true.toString)
    session.sparkContext.setLocalProperty(NativeFormatKey, "parquet")
    session.sparkContext.setLocalProperty(NativeDeltaWriteKey, true.toString)
  }

  def disable(session: SparkSession): Unit = {
    session.sparkContext.setLocalProperty(NativeApplicableKey, null)
    session.sparkContext.setLocalProperty(NativeFormatKey, null)
    session.sparkContext.setLocalProperty(NativeDeltaWriteKey, null)
  }
}
