/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, OmniHudiScanExecTransformer}

import org.apache.spark.sql.execution.SparkPlan

object PushDownFilterToOmniHudiScan {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter @ FilterExecTransformer(_, scan: OmniHudiScanExecTransformer) =>
      PushDownFilterToOmniScanHelper.tryPushDown(
        filter,
        scan.dataFilters,
        (output, dataFilters) => scan.copy(output = output, dataFilters = dataFilters))
    case _ =>
      None
  }
}
