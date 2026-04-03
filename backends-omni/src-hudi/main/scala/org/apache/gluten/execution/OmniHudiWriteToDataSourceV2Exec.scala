/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.omni.HudiWriteUtil

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite

/**
 * Replaces [[WriteToDataSourceV2Exec]] when the outer [[Write]] is Hudi, so micro-batch / DSv2
 * entry paths use the same Omni columnar writer factory as other Hudi writes.
 */
case class OmniHudiWriteToDataSourceV2Exec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    override val batchWrite: BatchWrite,
    writeMetrics: Seq[CustomMetric])
  extends AbstractHudiWriteExec {

  override val customMetrics: Map[String, SQLMetric] = {
    writeMetrics.map { m => m.name() -> SQLMetrics.createV2CustomMetric(sparkContext, m) }.toMap ++
      BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniHudiWriteToDataSourceV2Exec {

  private def extractOuterWrite(batchWrite: BatchWrite): Option[Write] = {
    batchWrite match {
      case microBatchWrite: MicroBatchWrite =>
        try {
          val streamWrite = microBatchWrite.writeSupport
          val outerClassField = streamWrite.getClass.getDeclaredField("this$0")
          outerClassField.setAccessible(true)
          outerClassField.get(streamWrite) match {
            case w: Write => Some(w)
            case _ => None
          }
        } catch {
          case _: Throwable => None
        }
      case _ => None
    }
  }

  def apply(original: WriteToDataSourceV2Exec): Option[OmniHudiWriteToDataSourceV2Exec] = {
    extractOuterWrite(original.batchWrite)
      .filter(HudiWriteUtil.supportsWrite)
      .map { w =>
        OmniHudiWriteToDataSourceV2Exec(
          original.query,
          original.refreshCache,
          w,
          original.batchWrite,
          original.writeMetrics
        )
      }
  }
}
