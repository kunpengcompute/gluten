/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.OverwritePartitionsDynamicExec

case class OmniHudiOverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractHudiWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniHudiOverwritePartitionsDynamicExec {
  def apply(original: OverwritePartitionsDynamicExec): OmniHudiOverwritePartitionsDynamicExec = {
    OmniHudiOverwritePartitionsDynamicExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
