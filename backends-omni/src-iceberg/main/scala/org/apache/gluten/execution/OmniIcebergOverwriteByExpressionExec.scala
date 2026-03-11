/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.OverwriteByExpressionExec

/**
 * Omni backend Iceberg OverwriteByExpression write Exec: replaces OverwriteByExpressionExec to use
 * columnar write.
 */
case class OmniIcebergOverwriteByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractIcebergWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(query = newChild)
}

object OmniIcebergOverwriteByExpressionExec {
  def apply(original: OverwriteByExpressionExec): IcebergWriteExec = {
    OmniIcebergOverwriteByExpressionExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
