/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.spark.sql.execution.SparkPlan

/**
 * Detects Delta write physical plans without directly linking main Omni code to Delta internals.
 *
 * The helper stays small and reflection/name based because it is loaded only when the Delta profile
 * is enabled, while the rest of the backend can remain Delta-optional.
 */
object DeltaWriteUtil {
  private val AppendDataExecV1ClassName =
    "org.apache.spark.sql.execution.datasources.v2.AppendDataExecV1"

  def isDeltaAppendDataExecV1(plan: SparkPlan): Boolean = {
    if (plan == null || plan.getClass.getName != AppendDataExecV1ClassName) {
      return false
    }
    val planText = plan.toString().toLowerCase(java.util.Locale.ROOT)
    planText.contains("deltatablev2") || planText.contains("writeintodeltabuilder")
  }
}
