/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.StructType

/**
 * Wraps Omni's transform rule and leaves Delta metadata plans on Spark's JVM execution path.
 *
 * Delta log replay may contain JSON scans, RDD state scans, and complex action schemas that are
 * not data-file reads. Skipping these subtrees prevents them from entering Omni native operators
 * while regular Delta Parquet table scans can still be offloaded.
 */
object DeltaMetadataTransformInterceptor {
  def apply(delegate: Rule[SparkPlan]): Rule[SparkPlan] = {
    new DeltaMetadataTransformInterceptorRule(delegate)
  }
}

/** Rule implementation used by [[DeltaMetadataTransformInterceptor]] to detect metadata subtrees. */
private class DeltaMetadataTransformInterceptorRule(delegate: Rule[SparkPlan])
  extends Rule[SparkPlan]
  with AdaptiveSparkPlanHelper
  with Logging {

  override val ruleName: String = delegate.ruleName

  override def apply(plan: SparkPlan): SparkPlan = {
    if (containsDeltaMetadataPlan(plan)) {
      logDebug(
        s"[Gluten][Delta] skip Omni transform for Delta metadata plan; rule=${delegate.ruleName}")
      plan
    } else {
      delegate(plan)
    }
  }

  private def containsDeltaMetadataPlan(plan: SparkPlan): Boolean = {
    collect(plan) {
      case rddScanExec: RDDScanExec if rddScanExec.nodeName.contains("Delta Table State") =>
        true
      case scanExec: FileSourceScanExec if isDeltaLogFileScan(scanExec) =>
        true
      case sparkPlan if isDeltaMetadataStatePlan(sparkPlan) =>
        true
    }.headOption.getOrElse(false)
  }

  private def isDeltaLogFileScan(scanExec: FileSourceScanExec): Boolean = {
    val location = scanExec.relation.location
    val className = Option(location).map(_.getClass.getName).getOrElse("")
    val containsDeltaLogPath = Option(location)
      .map(
        _.inputFiles.exists(path =>
          path.contains("/_delta_log/") || path.contains("\\_delta_log\\")))
      .getOrElse(false)

    className == "org.apache.spark.sql.delta.DeltaLogFileIndex" ||
    className == "org.apache.spark.sql.delta.files.DeltaLogFileIndex" ||
    className.endsWith(".DeltaLogFileIndex") ||
    containsDeltaLogPath
  }

  private def isDeltaMetadataStatePlan(plan: SparkPlan): Boolean = {
    val deltaActionStructColumns =
      Set("add", "remove", "metaData", "protocol", "txn", "commitInfo", "cdc", "domainMetadata")
    val deltaAuxColumns =
      Set("version", "inCommitTimestamp", "add_path_canonical", "remove_path_canonical")

    val structActionColumns = plan.output.collect {
      case attr
        if deltaActionStructColumns.contains(attr.name) &&
          attr.dataType.isInstanceOf[StructType] =>
        attr.name
    }.toSet
    val auxColumns = plan.output.map(_.name).toSet.intersect(deltaAuxColumns)

    structActionColumns.size >= 2 && auxColumns.nonEmpty
  }
}
