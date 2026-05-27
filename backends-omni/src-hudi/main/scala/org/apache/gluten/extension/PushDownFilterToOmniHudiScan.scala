/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, OmniHudiScanExecTransformer}

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.SparkPlan

object PushDownFilterToOmniHudiScan extends PredicateHelper {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter @ FilterExecTransformer(_, scan: OmniHudiScanExecTransformer) =>
      val allConditions = splitConjunctivePredicates(filter.cond)
      val pushedConditions = PushDownFilterToOmniScan.getPushedFilter(allConditions)
      val remainingConditions = allConditions.filterNot(pushedConditions.toSet.contains)

      if (pushedConditions.isEmpty) {
        None
      } else {
        val newScan = scan.copy(output = filter.output, dataFilters = pushedConditions)
        if (newScan.doValidate().ok()) {
          if (remainingConditions.isEmpty) {
            Some(newScan)
          } else {
            Some(filter.copy(
              condition = remainingConditions.reduceOption(And).get,
              child = newScan))
          }
        } else {
          None
        }
      }
    case _ =>
      None
  }
}
