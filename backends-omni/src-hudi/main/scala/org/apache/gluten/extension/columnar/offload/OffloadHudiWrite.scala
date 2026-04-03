/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.backendsapi.omni.HudiWriteUtil
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.execution.{
  OmniHudiAppendDataExec,
  OmniHudiOverwriteByExpressionExec,
  OmniHudiOverwritePartitionsDynamicExec,
  OmniHudiReplaceDataExec,
  OmniHudiWriteToDataSourceV2Exec
}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{
  AppendDataExec,
  OverwriteByExpressionExec,
  OverwritePartitionsDynamicExec,
  ReplaceDataExec,
  WriteToDataSourceV2Exec
}

/**
 * Hudi DSv2 write offload: replace Spark V2 write Exec nodes with Omni columnar Hudi writes when
 * [[org.apache.spark.sql.connector.write.Write]] is Hudi (see [[HudiWriteUtil.supportsWrite]]).
 *
 * @since 2026
 */

case class OffloadHudiAppend() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case a: AppendDataExec if HudiWriteUtil.supportsWrite(a.write) =>
      logWarning(
        s"[Gluten][Hudi+Omni] Offload Hudi append write: AppendDataExec -> OmniHudiAppendDataExec " +
          s"(write=${a.write.getClass.getName}).")
      OmniHudiAppendDataExec(a)
    case other =>
      other
  }
}

case class OffloadHudiReplaceData() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: ReplaceDataExec if HudiWriteUtil.supportsWrite(r.write) =>
      logWarning(
        s"[Gluten][Hudi+Omni] Offload Hudi replace-data write: ReplaceDataExec -> OmniHudiReplaceDataExec.")
      OmniHudiReplaceDataExec(r)
    case other => other
  }
}

case class OffloadHudiOverwrite() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: OverwriteByExpressionExec if HudiWriteUtil.supportsWrite(r.write) =>
      logWarning(
        s"[Gluten][Hudi+Omni] Offload Hudi overwrite-by-expression: " +
          "OverwriteByExpressionExec -> OmniHudiOverwriteByExpressionExec.")
      OmniHudiOverwriteByExpressionExec(r)
    case other => other
  }
}

case class OffloadHudiOverwritePartitionsDynamic() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: OverwritePartitionsDynamicExec if HudiWriteUtil.supportsWrite(r.write) =>
      logWarning(
        s"[Gluten][Hudi+Omni] Offload Hudi dynamic partition overwrite: " +
          "OverwritePartitionsDynamicExec -> OmniHudiOverwritePartitionsDynamicExec.")
      OmniHudiOverwritePartitionsDynamicExec(r)
    case other => other
  }
}

case class OffloadHudiWriteToDataSourceV2() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: WriteToDataSourceV2Exec =>
      OmniHudiWriteToDataSourceV2Exec(r) match {
        case Some(omni) =>
          logWarning(
            s"[Gluten][Hudi+Omni] Offload Hudi WriteToDataSourceV2Exec -> OmniHudiWriteToDataSourceV2Exec.")
          omni
        case None => r
      }
    case other => other
  }
}

/** Collection of Hudi write offload rules (loaded via `HudiOffloadRegistry` when `-Phudi` is used). */
object OffloadHudiWrite {
  def offloads: Seq[OffloadSingleNode] = Seq(
    OffloadHudiAppend(),
    OffloadHudiReplaceData(),
    OffloadHudiOverwrite(),
    OffloadHudiOverwritePartitionsDynamic(),
    OffloadHudiWriteToDataSourceV2()
  )
}
