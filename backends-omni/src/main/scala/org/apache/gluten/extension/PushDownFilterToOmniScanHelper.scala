/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, ValidatablePlan}

import org.apache.spark.sql.catalyst.expressions.{
  And,
  Attribute,
  BinaryComparison,
  Expression,
  IsNotNull,
  IsNull,
  Literal,
  PredicateHelper
}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Shared helper for optional data-source scans whose Spark scan can miss dataFilters.
 *
 * Ordinary Omni scan removes FilterExec only for predicates already present in scan.dataFilters.
 * Hudi/Delta scans can arrive with empty dataFilters, so recover only conditions Spark already
 * proved null-safe in the Filter itself, e.g. `isnotnull(c) AND c = 1`.
 */
object PushDownFilterToOmniScanHelper extends PredicateHelper {
  /**
   * Push filters into a scan whose original dataFilters may be incomplete.
   *
   * Conditions already present in scanDataFilters are trusted, same as ordinary Omni scan. Extra
   * conditions recovered from FilterExec are limited to Spark-proven null-safe predicates, so the
   * caller can safely remove those predicates from the remaining FilterExec condition.
   */
  def tryPushDown[S <: SparkPlan with ValidatablePlan](
      filter: FilterExecTransformer,
      scanDataFilters: Seq[Expression],
      buildScan: (Seq[Attribute], Seq[Expression]) => S): Option[SparkPlan] = {
    val filterConditions = splitConjunctivePredicates(filter.cond)
    val pushedFromScan = PushDownFilterToOmniScan.getPushedFilter(scanDataFilters)
    val pushedFromFilter = PushDownFilterToOmniScan.getPushedFilter(filterConditions)
    val recoveredFromFilter = getRecoverableFilterConditions(pushedFromFilter, filterConditions)

    val scanFilters = (pushedFromScan ++ recoveredFromFilter).distinct
    val remainingConditions = filterConditions.filterNot(scanFilters.toSet.contains)

    if (scanFilters.isEmpty) {
      None
    } else {
      val newScan = buildScan(filter.output, scanFilters)
      if (newScan.doValidate().ok()) {
        if (remainingConditions.isEmpty) {
          Some(newScan)
        } else {
          Some(filter.copy(
            condition = remainingConditions.reduceOption(And).get,
            child = newScan))
        }
      } else {
        None
      }
    }
  }

  private def getRecoverableFilterConditions(
      pushedFilters: Seq[Expression],
      filterConditions: Seq[Expression]): Seq[Expression] = {
    // Spark inserts IsNotNull guards for many null-intolerant comparisons. Treat those guards as
    // proof that removing the matching comparison from FilterExec still preserves SQL NULL semantics.
    val notNullAttributes = filterConditions.collect {
      case IsNotNull(attr: Attribute) => attr
    }

    // Use semantic equality instead of exprId/object equality so normalized/canonicalized
    // attributes that refer to the same column still match.
    def hasNotNullGuard(attr: Attribute): Boolean =
      notNullAttributes.exists(_.semanticEquals(attr))

    pushedFilters.filter {
      case IsNull(_: Attribute) => true
      case IsNotNull(_: Attribute) => true
      case comparison: BinaryComparison =>
        // A comparison can reject NULL only if Spark already kept an explicit IsNotNull guard for
        // the compared column. Without that guard, keep FilterExec as the semantic fallback.
        comparedAttribute(comparison.left, comparison.right).exists(hasNotNullGuard)
      case _ => false
    }
  }

  // Only simple column-vs-literal comparisons are considered recoverable. More complex
  // expressions (OR/NOT/functions/column-vs-column) stay in FilterExec.
  private def comparedAttribute(left: Expression, right: Expression): Option[Attribute] = {
    (left, right) match {
      case (attr: Attribute, _: Literal) => Some(attr)
      case (_: Literal, attr: Attribute) => Some(attr)
      case _ => None
    }
  }
}
