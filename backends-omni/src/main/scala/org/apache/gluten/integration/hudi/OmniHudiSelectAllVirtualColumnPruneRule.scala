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
package org.apache.gluten.integration.hudi

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.HudiDatasourceDetection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Removes `_hoodie_*` virtual columns from a logical [[Project]] when it matches a `SELECT *`-style
 * full projection of the child output and the subtree reads a Hudi [[FileFormat]] (see
 * [[HudiDatasourceDetection]]).
 *
 * Registered as a Spark post-hoc resolution rule (see [[OmniRuleApi]]): if this ran only in the
 * optimizer, [[org.apache.spark.sql.catalyst.optimizer.EliminateProject]] would already have
 * removed the redundant `Project` and the plan would expose Hudi metadata columns only via the
 * leaf scan.
 *
 * Lives under [[src/main]] so the rule is always registered for the Omni backend (the `src-hudi`
 * profile only adds Hudi read/write adapters).
 */
object OmniHudiSelectAllVirtualColumnPruneRule {

  def build(session: SparkSession): Rule[LogicalPlan] =
    OmniHudiSelectAllVirtualColumnPruneRule(new GlutenConfig(session.sessionState.conf))
}

case class OmniHudiSelectAllVirtualColumnPruneRule(glutenConf: GlutenConfig) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!glutenConf.omniHudiSelectAllStripVirtualColumns) {
      return plan
    }
    plan.transformUp { case p: Project => maybePrune(p) }
  }

  private def maybePrune(p: Project): LogicalPlan = {
    if (!isFullCoverProject(p)) return p
    if (!logicalSubtreeIsHudiFileScan(p.child)) return p
    val outs = p.child.output
    if (!outs.exists(_.name.startsWith("_hoodie_"))) return p

    val kept =
      if (isPositionalStarProjection(p)) {
        outs.zip(p.projectList).filterNot(_._1.name.startsWith("_hoodie_")).map(_._2)
      } else {
        val hoodieIds = outs.filter(_.name.startsWith("_hoodie_")).map(_.exprId).toSet
        p.projectList.filter(e => underlyingExprId(e).forall(id => !hoodieIds.contains(id)))
      }
    if (kept.length == p.projectList.length) p else p.copy(projectList = kept)
  }

  /**
   * Resolved `SELECT *`: child output attrs are each referenced once. Allows `Cast` / `Alias` on
   * top of an [[AttributeReference]] (common with `ORDER BY` and type coercion).
   */
  private def isFullCoverProject(p: Project): Boolean = {
    val outs = p.child.output
    if (outs.isEmpty || p.projectList.length != outs.length) {
      return false
    }
    if (isPositionalStarProjection(p)) return true
    val outIds = outs.map(_.exprId).toSet
    val projIds = p.projectList.flatMap(underlyingExprId)
    projIds.length == p.projectList.length && outIds == projIds.toSet &&
    projIds.length == projIds.distinct.length
  }

  /** Typical analyzer `SELECT *`: column i in project refers to child.output(i). */
  private def isPositionalStarProjection(p: Project): Boolean = {
    val outs = p.child.output
    outs.length == p.projectList.length &&
      outs.zip(p.projectList).forall { case (o, e) => underlyingExprId(e).contains(o.exprId) }
  }

  private def underlyingExprId(e: Expression): Option[ExprId] = e match {
    case ar: AttributeReference => Some(ar.exprId)
    case a: Alias => underlyingExprId(a.child)
    case c: Cast => underlyingExprId(c.child)
    case _ => None
  }

  /** True if some [[LogicalRelation]] under `plan` uses a Hudi [[FileFormat]]. */
  private def logicalSubtreeIsHudiFileScan(plan: LogicalPlan): Boolean =
    plan.exists {
      case lr: LogicalRelation =>
        lr.relation match {
          case h: HadoopFsRelation =>
            HudiDatasourceDetection.isHudiSparkFileFormat(h.fileFormat)
          case _ => false
        }
      case _ => false
    }
}
