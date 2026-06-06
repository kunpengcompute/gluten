/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, IcebergScanTransformer}

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.SparkPlan

object PushDownFilterToOmniIcebergScan extends PredicateHelper {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter @ FilterExecTransformer(_, scan: IcebergScanTransformer) =>
      val allConditions = splitConjunctivePredicates(filter.cond)
      val pushedConditions = PushDownFilterToOmniScan.getPushedFilter(allConditions)
      val remainingConditions = allConditions.filterNot(pushedConditions.toSet.contains)

      if (pushedConditions.isEmpty) {
        None
      } else {
        scan.setPushDownFilters(Seq.empty)
        scan.setPushDownFilters(pushedConditions)
        if (scan.doValidate().ok()) {
          if (remainingConditions.isEmpty) {
            Some(scan)
          } else {
            Some(filter.copy(
              condition = remainingConditions.reduceOption(And).get,
              child = scan))
          }
        } else {
          scan.setPushDownFilters(Seq.empty)
          None
        }
      }
    case _ =>
      None
  }
}
