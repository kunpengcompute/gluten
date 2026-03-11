/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.spark.sql.datasources.v2

import org.apache.gluten.connector.write.ColumnarBatchDataWriterFactory

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.v2.StreamWriterCommitProgress
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/** Columnar write task result: num rows written + WriterCommitMessage */
case class DataWritingColumnarBatchSparkTaskResult(
    numRows: Long,
    writerCommitMessage: WriterCommitMessage)

/**
 * Columnar batch write task: creates DataWriter from factory, iterates ColumnarBatch calling write,
 * then commit to get WriterCommitMessage; on failure abort, close in finally.
 */
trait WritingColumnarBatchSparkTask[W <: DataWriter[ColumnarBatch]]
  extends Logging
  with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def run(
      factory: ColumnarBatchDataWriterFactory,
      context: TaskContext,
      iter: Iterator[ColumnarBatch],
      customMetrics: Map[String, SQLMetric]): DataWritingColumnarBatchSparkTaskResult = {
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val dataWriter = factory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0L
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        dataWriter match {
          case m: CustomTaskMetric =>
            CustomMetrics.updateMetrics(m.currentMetricsValues, customMetrics)
          case _ =>
        }
        val batch = iter.next()
        count += batch.numRows()
        write(dataWriter, batch)
      }

      dataWriter match {
        case m: CustomTaskMetric =>
          CustomMetrics.updateMetrics(m.currentMetricsValues, customMetrics)
        case _ =>
      }
      logInfo(s"Writer for partition ${context.partitionId()} is committing.")
      val msg = dataWriter.commit()
      logInfo(s"Committed partition $partId (task $taskId)")
      DataWritingColumnarBatchSparkTaskResult(count, msg)
    })(
      catchBlock = {
        logError(s"Aborting commit for partition $partId (task $taskId)")
        dataWriter.abort()
        logError(s"Aborted commit for partition $partId (task $taskId)")
      },
      finallyBlock = {
        dataWriter.close()
      }
    )
  }
}

/** Default impl: delegates to writer.write(batch) */
object DataWritingColumnarBatchSparkTask
  extends WritingColumnarBatchSparkTask[DataWriter[ColumnarBatch]] {

  override protected def write(writer: DataWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
    writer.write(batch)
  }
}

/** Stream write progress: builds StreamWriterCommitProgress from num output rows */
object StreamWriterCommitProgressUtil {
  def getStreamWriterCommitProgress(numOutputRows: Long): StreamWriterCommitProgress = {
    StreamWriterCommitProgress(numOutputRows)
  }
}
