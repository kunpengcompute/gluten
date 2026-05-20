/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec

/**
 * Omni backend Hudi Append write Exec (aligned with OmniIcebergAppendDataExec):
 * wraps Spark AppendDataExec for columnar write; uses HudiWriteJniWrapper (Parquet) and
 * HudiCommitMessageBuilder for commit.
 *
 * @since 2026
 * @param query       child plan producing columnar batches to write
 * @param refreshCache callback after write (same as Spark [[AppendDataExec]])
 * @param write       Spark DSv2 [[Write]] from Hudi
 */

case class OmniHudiAppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractHudiWriteExec {

  /** Shown in Spark UI / EXPLAIN so Hudi columnar write is distinguishable from generic AppendData. */
  override def nodeName: String = "OmniColumnarHudiWrite"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniHudiAppendDataExec {
  /** Builds Omni Hudi append exec from Spark's [[AppendDataExec]] (used by offload rule). */
  def apply(original: AppendDataExec): OmniHudiAppendDataExec = {
    OmniHudiAppendDataExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
