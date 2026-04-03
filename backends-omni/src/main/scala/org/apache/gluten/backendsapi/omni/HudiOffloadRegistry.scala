/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

/**
 * Aggregates Hudi columnar offload rules for the Omni backend.
 *
 * Scan rules come from the `gluten-hudi` module ([[org.apache.gluten.execution.OffloadHudiScan]]);
 * write rules from `backends-omni` `src-hudi` ([[org.apache.gluten.extension.columnar.offload.OffloadHudiWrite]]):
 * replace DSv2 [[org.apache.spark.sql.execution.datasources.v2.AppendDataExec]] (and related) with
 * [[org.apache.gluten.execution.OmniHudiAppendDataExec]] etc. for Omni-accelerated Hudi writes.
 * Uses reflection so the Omni module compiles when `-Phudi` is off.
 *
 * @since 2026
 */
object HudiOffloadRegistry {

  /**
   * Hudi file scan offload only. Must run **before** [[org.apache.gluten.extension.columnar.offload.OffloadOthers]],
   * which would otherwise replace [[org.apache.spark.sql.execution.FileSourceScanExec]] first and skip
   * [[org.apache.gluten.execution.HudiScanTransformer]].
   */
  def scanOffloads: Seq[OffloadSingleNode] = Seq.empty

  /** Hudi DSv2 write offloads ([[OffloadHudiWrite]]); run with other offloads after scan / Iceberg ordering in [[OmniRuleApi]]. */
  def writeOffloads: Seq[OffloadSingleNode] = loadWriteOffloads()

  def offloads: Seq[OffloadSingleNode] = scanOffloads ++ writeOffloads

  private def loadScanOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.execution.OffloadHudiScan")
      val ctor = clazz.getConstructor()
      val instance = ctor.newInstance().asInstanceOf[OffloadSingleNode]
      Seq(instance)
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadWriteOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadHudiWrite")
      val method = clazz.getMethod("offloads")
      method.invoke(null).asInstanceOf[Seq[OffloadSingleNode]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }
}
