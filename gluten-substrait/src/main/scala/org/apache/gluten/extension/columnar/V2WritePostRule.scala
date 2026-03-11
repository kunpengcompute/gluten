/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.ColumnarV2TableWriteExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

/**
 * V2 columnar write post-rule: when the columnar write's child is AQE and does not declare
 * columnar support, set AQE to supportsColumnar=true to avoid redundant c2r -> AQE -> r2c -> writer.
 */
case class V2WritePostRule() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case write: ColumnarV2TableWriteExec =>
      // If columnar write's child is AQE and does not support columnar, make AQE columnar to avoid c2r->aqe->r2c->writer
      write.query match {
        case aqe: AdaptiveSparkPlanExec if !aqe.supportsColumnar =>
          write.withNewQuery(aqe.copy(supportsColumnar = true))
        case _ => write
      }
    case other => other
  }
}
