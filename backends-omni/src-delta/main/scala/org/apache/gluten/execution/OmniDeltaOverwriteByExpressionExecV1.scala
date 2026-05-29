/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Wraps Spark's Delta overwrite-by-expression command.
 *
 * Spark 3.5 + Delta 3.2 plans INSERT OVERWRITE as OverwriteByExpressionExecV1. Delta keeps the
 * overwrite transaction and commit semantics, while Omni native Parquet write is enabled during
 * execution.
 */
case class OmniDeltaOverwriteByExpressionExecV1(original: SparkPlan) extends LeafV2CommandExec {

  override def nodeName: String = "OmniDeltaOverwriteByExpressionExecV1"

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val sparkSession = SparkSession.active
    DeltaNativeParquetWrite.enable(sparkSession)
    try {
      original.executeCollect()
      Nil
    } finally {
      DeltaNativeParquetWrite.disable(sparkSession)
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new IllegalArgumentException("OmniDeltaOverwriteByExpressionExecV1 is a leaf node")
    }
    this
  }
}
