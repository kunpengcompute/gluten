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
package org.apache.gluten.extension

import org.apache.gluten.expression.ExpressionMappings
import org.apache.gluten.expression.aggregate.{OmniCollectList, OmniCollectSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, AGGREGATE_EXPRESSION}

import scala.reflect.{classTag, ClassTag}

/**
 * for replace the collect_set & collect_list agg func to omni_collect_set & omni_collect_list
 */
case class OmniRewriteCollectFuncRule(spark: SparkSession) extends Rule[LogicalPlan] {
  import OmniRewriteCollectFuncRule._
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!has[OmniCollectSet] && !has[OmniCollectList]) {
      return plan
    }

    val newPlan = plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case node =>
        replaceAggCollect(node)
    }
    if (newPlan.fastEquals(plan)) {
      return plan
    }
    newPlan
  }

  private def replaceAggCollect(node: LogicalPlan): LogicalPlan = {
    node match {
      case agg: Aggregate =>
        agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
          case ToOmniCollect(newAggExpr) =>
            newAggExpr
        }
      case other => other
    }
  }
}

object OmniRewriteCollectFuncRule {
  private object ToOmniCollect {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case aggExpr @ AggregateExpression(s: CollectSet, _, _, _, _) if has[OmniCollectSet] =>
        val newAggExpr =
          aggExpr.copy(aggregateFunction = OmniCollectSet(s.child))
        Some(newAggExpr)
      case aggExpr @ AggregateExpression(l: CollectList, _, _, _, _) if has[OmniCollectList] =>
        val newAggExpr = aggExpr.copy(OmniCollectList(l.child))
        Some(newAggExpr)
      case _ => None
    }
  }

  private def has[T <: Expression: ClassTag]: Boolean =
    ExpressionMappings.expressionsMap.contains(classTag[T].runtimeClass)
}
