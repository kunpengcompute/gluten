/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.OverwritePartitionsDynamicExec

/**
 * Omni backend Iceberg OverwritePartitionsDynamic write Exec: replaces OverwritePartitionsDynamicExec
 * to use columnar write.
 */
case class OmniIcebergOverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractIcebergWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(query = newChild)
}

object OmniIcebergOverwritePartitionsDynamicExec {
  def apply(original: OverwritePartitionsDynamicExec): IcebergWriteExec = {
    OmniIcebergOverwritePartitionsDynamicExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
