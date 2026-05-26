/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Wraps Spark's Delta `AppendDataExecV1`.
 *
 * Spark 3.5 + Delta 3.2 plans SQL INSERT as AppendDataExecV1. We keep Delta's original plan in
 * charge of transaction commit, but enable Omni native Parquet write while it runs.
 */
case class OmniDeltaAppendDataExecV1(original: SparkPlan) extends LeafV2CommandExec {

  override def nodeName: String = "OmniDeltaAppendDataExecV1"

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
      throw new IllegalArgumentException("OmniDeltaAppendDataExecV1 is a leaf node")
    }
    this
  }
}
