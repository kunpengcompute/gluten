/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Hudi offload rules for Omni: scan via pre-transform (before [[org.apache.gluten.extension.columnar.offload.OffloadOthers]]),
 * write via reflection from `src-hudi`.
 */
object HudiOffloadRegistry {

  /** Write rules only; Hudi scan uses [[injectPreTransformRules]]. */
  def offloads: Seq[OffloadSingleNode] = loadWriteOffloads()

  def injectPreTransformRules(injector: LegacyInjector): Unit = {
    loadScanPreRule().foreach { rule =>
      injector.injectPreTransform(_ => rule)
    }
  }

  private def loadScanPreRule(): Option[Rule[SparkPlan]] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadOmniHudiScanPreRule$")
      Class.forName("org.apache.gluten.execution.HudiScanTransformer")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("apply")
      Some(method.invoke(module).asInstanceOf[Rule[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
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
