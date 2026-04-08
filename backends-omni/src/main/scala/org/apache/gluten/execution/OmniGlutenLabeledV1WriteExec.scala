/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the License); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, LeafV2CommandExec, OverwriteByExpressionExecV1}

/**
 * Delta V1 fallback writes use Spark's [[AppendDataExecV1]] / [[OverwriteByExpressionExecV1]].
 * We cannot subclass those (sealed [[org.apache.spark.sql.execution.datasources.v2.V1FallbackWriters]]),
 * so we replace them with a [[LeafV2CommandExec]] that keeps the **same** [[nodeName]] as Spark
 * (`AppendDataExecV1` / `OverwriteByExpressionExecV1`) and prepends a Gluten+Omni line to
 * [[stringArgs]] so EXPLAIN FORMATTED / UI "Arguments:" show the hint on that operator, not an
 * extra wrapper name. Execution delegates to the original exec (via [[executeCollect]] to avoid
 * calling protected [[V2CommandExec.run]] on another instance).
 */
case class OmniGlutenLabeledAppendDataExecV1(delegate: AppendDataExecV1) extends LeafV2CommandExec {
  override def nodeName: String = "OmniAppendDataExecV1"
  override def output: Seq[Attribute] = delegate.output
  override def stringArgs: Iterator[Any] = {
    Iterator.single(OmniGlutenLabeledV1WriteExec.OMNI_DELTA_APPEND_ARG_HINT) ++ delegate.productIterator
  }
  override protected def run(): Seq[InternalRow] = delegate.executeCollect().toSeq
}

case class OmniGlutenLabeledOverwriteByExpressionExecV1(delegate: OverwriteByExpressionExecV1)
  extends LeafV2CommandExec {
  override def nodeName: String = "OverwriteByExpressionExecV1"
  override def output: Seq[Attribute] = delegate.output
  override def stringArgs: Iterator[Any] = {
    Iterator.single(OmniGlutenLabeledV1WriteExec.OMNI_DELTA_OVERWRITE_ARG_HINT) ++ delegate.productIterator
  }
  override protected def run(): Seq[InternalRow] = delegate.executeCollect().toSeq
}

object OmniGlutenLabeledV1WriteExec {
  private[execution] val OMNI_DELTA_APPEND_ARG_HINT =
    "[Gluten+Omni Delta] (Omni + Gluten stack)"

  private[execution] val OMNI_DELTA_OVERWRITE_ARG_HINT =
    "[Gluten+Omni Delta] (Omni + Gluten stack)"
}
