/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{
  ColumnarBatchDataWriterFactory,
  ColumnarMicroBatchWriterFactory,
  ColumnarStreamingDataWriterFactory
}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.RowType

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}
import org.apache.spark.sql.datasources.v2.{
  DataWritingColumnarBatchSparkTask,
  DataWritingColumnarBatchSparkTaskResult,
  StreamWriterCommitProgressUtil
}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

/**
 * Columnar V2 table write exec: uses DataSource V2 BatchWrite and columnar RDD to run write
 * tasks and commit. Subclasses implement createBatchWriterFactory / createStreamingWriterFactory;
 * run() uses query.executeColumnar() + factory + batchWrite.commit(messages).
 */
trait ColumnarV2TableWriteExec extends V2TableWriteExec with ValidatablePlan {

  def refreshCache: () => Unit

  def write: Write

  def batchWrite: BatchWrite = write.toBatch

  def withNewQuery(newQuery: SparkPlan): SparkPlan = withNewChildInternal(newQuery)

  /** Factory that creates columnar DataWriters for batch write. */
  protected def createBatchWriterFactory(schema: StructType): ColumnarBatchDataWriterFactory

  /** Factory that creates columnar DataWriters for streaming/micro-batch write (with epochId). */
  protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory

  override protected def run(): Seq[InternalRow] = {
    writeColumnarBatchWithV2(batchWrite)
    refreshCache()
    Nil
  }

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def rowType0(): Convention.RowType = RowType.VanillaRow

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(
    ConventionReq.ofBatch(
      ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType)))

  /** Columnar write task impl: creates writer from factory, iterates ColumnarBatch to write, then commit. */
  private def writingTaskBatch = DataWritingColumnarBatchSparkTask

  /**
   * Performs columnar write with V2 BatchWrite: runs columnar RDD, runJob per partition to write,
   * collects WriterCommitMessage then calls batchWrite.commit(messages).
   * Uses ColumnarMicroBatchWriterFactory when batchWrite is MicroBatchWrite.
   */
  private def writeColumnarBatchWithV2(batchWrite: BatchWrite): Unit = {
    // When there are no partitions, build a single-partition RDD so at least one write task runs.
    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = query.executeColumnar()
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    val task = writingTaskBatch
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(
      s"Start processing data source write support: $batchWrite. " +
        s"The input RDD has ${messages.length} partitions.")

    val writeMetrics: Map[String, SQLMetric] = customMetrics
    val factory = batchWrite match {
      case m: MicroBatchWrite =>
        val epochIdField = m.getClass.getDeclaredField("epochId")
        epochIdField.setAccessible(true)
        val epochId = epochIdField.getLong(m)
        new ColumnarMicroBatchWriterFactory(epochId, createStreamingWriterFactory(query.schema))
      case _ =>
        createBatchWriterFactory(query.schema)
    }
    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(factory, context, iter, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingColumnarBatchSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(
        StreamWriterCommitProgressUtil.getStreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed", cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }
  }

  /** Custom metrics: V2 write supported metrics + backend genBatchWriteMetrics. */
  override val customMetrics: Map[String, SQLMetric] = {
    write
      .supportedCustomMetrics()
      .map {
        customMetric =>
          customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
      }
      .toMap ++ BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }
}
