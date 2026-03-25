/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.omni.HudiCommitMessageBuilder
import org.apache.gluten.execution.HudiWriteJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Omni Hudi columnar DataWriter (aligned with OmniIcebergColumnarBatchDataWriter):
 * write() passes ColumnarBatch to HudiWriteJniWrapper (Parquet writer);
 * commit() gets file info JSON from jniWrapper.commit(), builds Hudi WriterCommitMessage.
 *
 * @since 2026
 */

case class OmniHudiColumnarBatchDataWriter(jniWrapper: HudiWriteJniWrapper)
  extends DataWriter[ColumnarBatch]
  with Logging {

  /** Delegates to native Parquet writer via [[HudiWriteJniWrapper]]. */
  override def write(batch: ColumnarBatch): Unit = {
    jniWrapper.write(batch)
  }

  /**
   * Closes files, collects per-file JSON stats, and builds Hudi [[WriterCommitMessage]]
   * for the driver commit phase.
   */
  override def commit(): WriterCommitMessage = {
    val fileInfoJsonArray = jniWrapper.commit()
    HudiCommitMessageBuilder.buildCommitMessage(fileInfoJsonArray)
  }

  /** Best-effort abort; underlying native resources are released on failed tasks as per Spark contract. */
  override def abort(): Unit = {
    logInfo("Abort OmniHudiColumnarBatchDataWriter")
  }

  override def close(): Unit = {
    logDebug("Close OmniHudiColumnarBatchDataWriter")
  }

  /** Exposes batch write metrics from the JNI wrapper for Spark SQL metrics UI. */
  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val m = jniWrapper.metrics()
    if (m == null) Array.empty[CustomTaskMetric]
    else m.toCustomTaskMetrics
  }
}
