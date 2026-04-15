/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

/**
 * Aggregates Hudi columnar offload rules for the Omni backend.
 *
 * Scan rules come from the `gluten-hudi` module ([[org.apache.gluten.execution.OffloadHudiScan]]);
 * write rules from `backends-omni` `src-hudi` ([[org.apache.gluten.extension.columnar.offload.OffloadHudiWrite]]).
 * Uses reflection so the Omni module compiles when `-Phudi` is off.
 *
 * @since 2026
 */

object HudiOffloadRegistry {

  /** All Hudi-related [[OffloadSingleNode]] rules (scan + write); empty seq if classes are absent. */
  def offloads: Seq[OffloadSingleNode] = {
    val scanOffloads = loadScanOffloads()
    val writeOffloads = loadWriteOffloads()
    scanOffloads ++ writeOffloads
  }

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
