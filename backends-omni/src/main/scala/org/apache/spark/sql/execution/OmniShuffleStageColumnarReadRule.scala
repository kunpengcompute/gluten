/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.gluten.execution.WholeStageTransformer
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import scala.annotation.tailrec

/**
 * After [[ColumnarCollapseTransformStages]], [[InputIteratorTransformer]]
 * may wrap [[ColumnarInputAdapter]]([[ShuffleQueryStageExec]]). [[ShuffleQueryStageExec]] does not
 * implement columnar execution; [[ColumnarInputAdapter#doExecuteColumnar]] forwards to it and hits
 * Spark's default error. Insert [[OmniAQEShuffleReadExec]] (same path as [[RewriteAQEShuffleRead]]
 * for [[AQEShuffleReadExec]]) so shuffle output is read via [[OmniColumnarShuffleExchangeExec#getShuffleRDD]].
 *
 * Lives in this package so [[InputIteratorTransformer]] / [[ColumnarInputAdapter]] resolve reliably
 * (same package as gluten-substrait); wildcard imports from `org.apache.gluten.extension` do not.
 */
object OmniShuffleStageColumnarReadRule extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val wrapped = plan.transformUp(wrapShuffleStages)
    flattenRedundantOmniAqe(wrapped, MaxOmniAqeFlattenRounds)
  }

  private final val MaxOmniAqeFlattenRounds = 8

  /** [[RewriteAQEShuffleRead]] plus this rule can stack; collapse duplicate [[OmniAQEShuffleReadExec]]. */
  @tailrec
  private def flattenRedundantOmniAqe(plan: SparkPlan, roundsLeft: Int): SparkPlan = {
    if (roundsLeft <= 0) {
      plan
    } else {
      val next = plan.transformUp {
        case outer: OmniAQEShuffleReadExec =>
          findNestedOmniAqe(outer.child) match {
            case Some(inner) if inner ne outer =>
              OmniAQEShuffleReadExec(inner.child, inner.partitionSpecs)
            case _ => outer
          }
      }
      if (next fastEquals plan) next
      else flattenRedundantOmniAqe(next, roundsLeft - 1)
    }
  }

  /** First [[OmniAQEShuffleReadExec]] under wrappers (same chain as shuffle read stage lookup). */
  private def findNestedOmniAqe(p: SparkPlan): Option[OmniAQEShuffleReadExec] = p match {
    case o: OmniAQEShuffleReadExec => Some(o)
    case it: InputIteratorTransformer => findNestedOmniAqe(it.child)
    case ca: ColumnarInputAdapter => findNestedOmniAqe(ca.child)
    case wst: WholeStageTransformer => findNestedOmniAqe(wst.child)
    case _ => None
  }

  private val wrapShuffleStages: PartialFunction[SparkPlan, SparkPlan] = {
    case adapter: ColumnarInputAdapter =>
      adapter.child match {
        // [[RewriteAQEShuffleRead]] or a prior pass may already wrap the stage; do not nest again.
        case _: OmniAQEShuffleReadExec =>
          adapter
        case stage: ShuffleQueryStageExec =>
          omniShuffle(stage).map { omni =>
            val n = omni.columnarShuffleDependency.partitioner.numPartitions
            val specs = (0 until n).map(i => CoalescedPartitionSpec(i, i + 1))
            adapter.copy(child = OmniAQEShuffleReadExec(stage, specs))
          }.getOrElse(adapter)
        case _ => adapter
      }
    case it: InputIteratorTransformer =>
      it.child match {
        case o: OmniAQEShuffleReadExec =>
          // AQE may replace the child via copy and skip IIT#withNewChildInternal, leaving a bare
          // OmniAQEShuffleReadExec; normalize to match wrapInputIteratorTransformer.
          it.copy(child = ColumnarInputAdapter(o))
        case ca: ColumnarInputAdapter if ca.child.isInstanceOf[OmniAQEShuffleReadExec] =>
          it
        case stage: ShuffleQueryStageExec =>
          omniShuffle(stage).map { omni =>
            val n = omni.columnarShuffleDependency.partitioner.numPartitions
            val specs = (0 until n).map(i => CoalescedPartitionSpec(i, i + 1))
            it.copy(child = ColumnarInputAdapter(OmniAQEShuffleReadExec(stage, specs)))
          }.getOrElse(it)
        case _ => it
      }
  }

  private def omniShuffle(stage: ShuffleQueryStageExec): Option[OmniColumnarShuffleExchangeExec] = {
    stage.shuffle match {
      case o: OmniColumnarShuffleExchangeExec => Some(o)
      case r: ReusedExchangeExec =>
        r.child match {
          case o: OmniColumnarShuffleExchangeExec => Some(o)
          case _ => None
        }
      case _ => None
    }
  }
}
