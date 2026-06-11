/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, IcebergScanTransformer}

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SparkPlan

object PushDownFilterToOmniIcebergScan {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter @ FilterExecTransformer(_, scan: IcebergScanTransformer) =>
      PushDownFilterToOmniScanHelper.tryPushDown(
        filter,
        Seq.empty,
        (output, dataFilters) => {
          val newScan = scan.copy(output = output.map(_.asInstanceOf[AttributeReference]))
          newScan.setPushDownFilters(dataFilters)
          newScan
        })
    case _ =>
      None
  }
}
