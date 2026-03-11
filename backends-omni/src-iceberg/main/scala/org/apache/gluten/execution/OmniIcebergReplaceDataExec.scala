/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec

/**
 * Omni backend Iceberg ReplaceData write Exec: replaces ReplaceDataExec to use columnar write.
 */
case class OmniIcebergReplaceDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractIcebergWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(query = newChild)
}

object OmniIcebergReplaceDataExec {
  def apply(original: ReplaceDataExec): IcebergWriteExec = {
    OmniIcebergReplaceDataExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
