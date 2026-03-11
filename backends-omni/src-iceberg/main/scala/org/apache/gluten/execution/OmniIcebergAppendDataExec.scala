/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec

/**
 * Omni backend Iceberg Append write Exec: wraps Spark AppendDataExec for columnar write + Iceberg
 * commit. Replaces AppendDataExec via OffloadIcebergAppend rule.
 */
case class OmniIcebergAppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractIcebergWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(query = newChild)
}

object OmniIcebergAppendDataExec {
  /** Builds OmniIcebergAppendDataExec from the original AppendDataExec. */
  def apply(original: AppendDataExec): IcebergWriteExec = {
    OmniIcebergAppendDataExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
