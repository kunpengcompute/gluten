/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, OmniDeltaScanExecTransformer}

import org.apache.spark.sql.execution.SparkPlan

/**
 * Delta-specific extension for [[PushDownFilterToOmniScan]].
 *
 * It pushes supported Omni filter predicates into [[OmniDeltaScanExecTransformer]] while keeping
 * the implementation in `src-delta`, so the default Omni build does not depend on Delta classes.
 */
object PushDownFilterToOmniDeltaScan {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter @ FilterExecTransformer(_, scan: OmniDeltaScanExecTransformer) =>
      PushDownFilterToOmniScanHelper.tryPushDown(
        filter,
        scan.dataFilters,
        (output, dataFilters) => scan.copy(output = output, dataFilters = dataFilters))
    case _ =>
      None
  }
}
