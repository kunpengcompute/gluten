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
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.columnarbatch.OmniBatch
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow}
import org.apache.gluten.extension.columnar.V2WritePostRule
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers, OffloadWrite}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.gluten.extension.{OmniHLLRewriteRule, RewriteAQEShuffleRead}
import org.apache.gluten.integration.hudi.OmniHudiSelectAllVirtualColumnPruneRule
import org.apache.spark.sql.catalyst.optimizer.{CombineJoinedAggregates, DedupLeftSemiJoin, MergeSubqueryFilters, PushOrderedLimitThroughAgg, ReorderJoinEnhances, RewriteSelfJoinInInPredicate, RollupOptimization, ShuffleJoinStrategy, RewriteTopNSort, CombineWindowSort, OmniRewriteSubqueryBroadcast, CombineProject}
import org.apache.gluten.extension.{AdaptiveHashAggregateRule, FallbackBroadcastHashJoin, FallbackBroadcastHashJoinPrepQueryStage, OmniRewriteCollectFuncRule, OmniRewriteJoin, PushDownFilterToOmniScan}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{
  ColumnarCollapseTransformStages,
  FileSourceScanExec,
  GlutenAutoAdjustStageResourceProfile,
  GlutenFallbackReporter,
  OmniShuffleStageColumnarReadRule,
  RDDScanExec,
  SparkPlan
}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.StructType

class OmniRuleApi extends RuleApi {

  import OmniRuleApi._

  override def injectRules(injector: Injector): Unit = {
    // Same as VeloxIcebergComponent: register BatchScanExec -> IcebergScanTransformer (legacy + RAS).
    // gluten-iceberg is optional (-Piceberg); reflect to keep default Omni build compiling.
    injectIcebergScanIfPresent(injector)
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

object OmniRuleApi {

  private def injectIcebergScanIfPresent(injector: Injector): Unit = {
    try {
      val clazz = Class.forName("org.apache.gluten.execution.OffloadIcebergScan$")
      val module = clazz.getField("MODULE$").get(null)
      val m = clazz.getMethod("inject", classOf[Injector])
      m.invoke(module, injector)
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
      case e: java.lang.reflect.InvocationTargetException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectQueryStagePrepRule(FallbackMultiCodegens.apply)
    injector.injectQueryStagePrepRule(FallbackBroadcastHashJoinPrepQueryStage.apply)
    injector.injectQueryStagePrepRule(DedupLeftSemiJoin.apply)
    // AQE replaces exchanges with ShuffleQueryStageExec / AQEShuffleReadExec; without re-collapsing,
    // native HashAggregate can sit outside WholeStageTransformer while still reporting columnar
    // support, which triggers SparkPlan's default doExecuteColumnar ("column support mismatch").
    // Do not touch session.sessionState in this lambda: Spark builds QueryStagePrep rules while
    // initializing sessionState; reading sessionState here causes infinite recursion (StackOverflow).
    // Must not evaluate [[GlutenConfig(session.sessionState.conf)]] when the rule is built:
    // [[InjectorControl.wrapRule]] calls ruleBuilder(session) eagerly; touching sessionState here
    // recurses while sessionState is still initializing (StackOverflow).
    injector.injectQueryStagePrepRule { session =>
      new Rule[SparkPlan] {
        private lazy val collapse =
          ColumnarCollapseTransformStages(new GlutenConfig(session.sessionState.conf))
        override def apply(plan: SparkPlan): SparkPlan = collapse(plan)
      }
    }
    injector.injectPlannerStrategy(_ => ShuffleJoinStrategy)
    injector.injectOptimizerRule(OmniRewriteCollectFuncRule.apply)
    injector.injectOptimizerRule(OmniHLLRewriteRule.apply)
//    injector.injectOptimizerRule(CollapseGetJsonObjectExpressionRule.apply)
//    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
    injector.injectOptimizerRule(ReorderJoinEnhances.apply)
    injector.injectOptimizerRule(RewriteSelfJoinInInPredicate.apply)
    injector.injectOptimizerRule(CombineJoinedAggregates.apply)
    injector.injectOptimizerRule(MergeSubqueryFilters.apply)
    injectHudiInsertSqlRewriteIfPresent(injector)
    // Must run in analyzer post-hoc, not the optimizer: trivial `SELECT *` Project (same output as
    // Relation) is removed by EliminateProject before typical extended optimizer rules see it.
    injector.injectPostHocResolutionRule(OmniHudiSelectAllVirtualColumnPruneRule.build)
  }

  /**
   * When `-Phudi` is on, rewrite `InsertIntoHoodieTableCommand` -> `writeTo().append()` so INSERT SQL
   * can use DSv2 + [[org.apache.gluten.execution.OmniHudiAppendDataExec]] without patching Hudi JAR.
   */
  private def injectHudiInsertSqlRewriteIfPresent(injector: SparkInjector): Unit = {
    try {
      val clazz = Class.forName("org.apache.gluten.integration.hudi.OmniHudiInsertRewriteRule$")
      val mod = clazz.getField("MODULE$").get(null)
      val build = clazz.getMethod("build", classOf[org.apache.spark.sql.SparkSession])
      injector.injectResolutionRule { session =>
        build.invoke(mod, session).asInstanceOf[Rule[LogicalPlan]]
      }
      injector.injectOptimizerRule { session =>
        build.invoke(mod, session).asInstanceOf[Rule[LogicalPlan]]
      }
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
      case e: java.lang.reflect.InvocationTargetException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(_ => OmniRewriteJoin())
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(c => PushOrderedLimitThroughAgg(c.session))
    injector.injectPreTransform(_ => OmniRewriteSubqueryBroadcast())
    injector.injectPreTransform(_ => RewriteAQEShuffleRead())
    injector.injectPreTransform(c => FallbackBroadcastHashJoin.apply(c.session))
//    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
//    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Legacy: The legacy transform rule.
    // Hudi scan offload must precede OffloadOthers so FileSourceScanExec can become HudiScanTransformer.
    // Hudi DSv2 writes: OffloadHudiWrite -> OmniHudi*Exec when plan uses AppendDataExec / Overwrite* / WriteToDataSourceV2 (use writeTo / V2 catalog; INSERT may stay Command).
    val offloads = HudiOffloadRegistry.scanOffloads ++
      Seq(OffloadOthers(), OffloadExchange(), OffloadJoin(), OffloadWrite()) ++
      IcebergOffloadRegistry.offloads ++
      HudiOffloadRegistry.writeOffloads
    val validatorBuilder: GlutenConfig => Validator = conf =>
      Validators.newValidator(conf, offloads)
    val rewrites =
      Seq(
        RewriteIn,
        RewriteMultiChildrenCount,
        OmniGeneratePullOutPreProject,
        PullOutPostProject,
        ProjectColumnPruning)
    injector.injectTransform(
      c => intercept(HeuristicTransform.WithRewrites(validatorBuilder(c.glutenConf), rewrites, offloads)))

    // Legacy: Post-transform rules.
    injector.injectPostTransform(_ => V2WritePostRule())
    injector.injectPostTransform(_ => UnionTransformerRule())
//    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToOmniScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(_ => CombineProject())
//    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => AdaptiveHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, OmniBatch))
    injector.injectPostTransform(_ => RewriteTopNSort())
    injector.injectPostTransform(_ => CombineWindowSort())
    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.glutenConf))
    // Omni-only: same behavior as previous postColumnarCollapse hook, without touching shared
    // [[ColumnarCollapseTransformStages]] / [[BackendSettingsApi]] (Velox/CH substrait parity).
    injector.injectPost(_ => OmniShuffleStageColumnarReadRule)

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RollupOptimization.apply(c.session))
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenAutoAdjustStageResourceProfile(c.glutenConf, c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  private def injectRas(injector: RasInjector): Unit = {
    // Gluten RAS: Pre rules.
//    injector.injectPreTransform(_ => RemoveTransitions)
//    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
//    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
//    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
//    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
//    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
//    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))
//
//    // Gluten RAS: The RAS rule.
//    val validatorBuilder: GlutenConfig => Validator = conf => Validators.newValidator(conf)
//    val rewrites =
//      Seq(
//        RewriteIn,
//        RewriteMultiChildrenCount,
//        RewriteJoin,
//        PullOutPreProject,
//        PullOutPostProject,
//        ProjectColumnPruning)
//    injector.injectCoster(_ => LegacyCoster)
//    injector.injectCoster(_ => RoughCoster)
//    injector.injectCoster(_ => RoughCoster2)
//    injector.injectRasRule(_ => RemoveSort)
//    val offloads: Seq[RasOffload] = Seq(
//      RasOffload.from[Exchange](OffloadExchange()),
//      RasOffload.from[BaseJoinExec](OffloadJoin()),
//      RasOffload.from[FilterExec](OffloadOthers()),
//      RasOffload.from[ProjectExec](OffloadOthers()),
//      RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()),
//      RasOffload.from[DataSourceScanExec](OffloadOthers()),
//      RasOffload.from(HiveTableScanExecTransformer.isHiveTableScan(_))(OffloadOthers()),
//      RasOffload.from[CoalesceExec](OffloadOthers()),
//      RasOffload.from[HashAggregateExec](OffloadOthers()),
//      RasOffload.from[SortAggregateExec](OffloadOthers()),
//      RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
//      RasOffload.from[UnionExec](OffloadOthers()),
//      RasOffload.from[ExpandExec](OffloadOthers()),
//      RasOffload.from[WriteFilesExec](OffloadOthers()),
//      RasOffload.from[SortExec](OffloadOthers()),
//      RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
//      RasOffload.from[WindowExec](OffloadOthers()),
//      RasOffload.from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec(_))(OffloadOthers()),
//      RasOffload.from[LimitExec](OffloadOthers()),
//      RasOffload.from[GenerateExec](OffloadOthers()),
//      RasOffload.from[EvalPythonExec](OffloadOthers()),
//      RasOffload.from[SampleExec](OffloadOthers())
//    )
//    offloads.foreach(
//      offload =>
//        injector.injectRasRule(
//          c => RasOffload.Rule(offload, validatorBuilder(c.glutenConf), rewrites)))
//
//    // Gluten RAS: Post rules.
//    injector.injectPostTransform(_ => RemoveTransitions)
//    injector.injectPostTransform(_ => UnionTransformerRule())
//    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
//    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
//    injector.injectPostTransform(_ => PushDownFilterToScan)
//    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
//    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
//    injector.injectPostTransform(_ => EliminateLocalSort)
//    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
//    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
//    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatch))
//    injector.injectPostTransform(
//      c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
//    SparkShimLoader.getSparkShims
//      .getExtendedColumnarPostRules()
//      .foreach(each => injector.injectPostTransform(c => each(c.session)))
//    injector.injectPostTransform(c => ColumnarCollapseTransformStages(c.glutenConf))
//    injector.injectPostTransform(c => RemoveGlutenTableCacheColumnarToRow(c.session))
//    injector.injectPostTransform(c => GlutenFallbackReporter(c.glutenConf, c.session))
//    injector.injectPostTransform(_ => RemoveFallbackTagRule())
  }

  /**
   * Delta metadata reconstruction currently reads `_delta_log` JSON and builds state rows with
   * nested structs/maps. Omni native scan/shuffle does not support that path yet, and forcing
   * Gluten transform on those internal plans leads to:
   *   1. JsonReadFormat validation warnings
   *   2. anonymous ScalaUDF validation failures
   *   3. rowSplit native errors on complex action rows
   *
   * Keep those internal metadata plans on Spark row execution while leaving normal Delta data-file
   * scans available for the usual Omni offload path.
   */
  private class OmniSparkRuleInterceptor(delegate: Rule[SparkPlan])
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper
    with Logging {

    override val ruleName: String = delegate.ruleName

    override def apply(plan: SparkPlan): SparkPlan = {
      findSkipReason(plan) match {
        case Some(reason) =>
          logWarning(
            s"[Omni-Proof][DeltaRead] skipping Omni transform for Delta metadata plan; " +
              s"rule=${delegate.ruleName}, reason=$reason")
          plan
        case None =>
          delegate(plan)
      }
    }

    private def findSkipReason(plan: SparkPlan): Option[String] = {
      collect(plan) {
        case rddScanExec: RDDScanExec if rddScanExec.nodeName.contains("Delta Table State") =>
          s"RDD scan '${rddScanExec.nodeName}' is Delta Table State"
        case scanExec: FileSourceScanExec if isDeltaLogFileScan(scanExec) =>
          s"File scan '${scanExec.nodeName}' reads Delta log files"
        case sparkPlan if isDeltaMetadataStatePlan(sparkPlan) =>
          val output = sparkPlan.output.map(_.name).mkString("[", ",", "]")
          s"plan '${sparkPlan.nodeName}' has Delta metadata action schema output=$output"
      }.headOption
    }

    private def isDeltaLogFileScan(scanExec: FileSourceScanExec): Boolean = {
      val location = scanExec.relation.location
      val className = Option(location).map(_.getClass.getName).getOrElse("")
      val containsDeltaLogPath = Option(location)
        .map(
          _.inputFiles.exists(path =>
            path.contains("/_delta_log/") || path.contains("\\_delta_log\\")))
        .getOrElse(false)

      className == "org.apache.spark.sql.delta.DeltaLogFileIndex" ||
      className == "org.apache.spark.sql.delta.files.DeltaLogFileIndex" ||
      className.endsWith(".DeltaLogFileIndex") ||
      containsDeltaLogPath
    }

    private def isDeltaMetadataStatePlan(plan: SparkPlan): Boolean = {
      val deltaActionStructColumns =
        Set("add", "remove", "metaData", "protocol", "txn", "commitInfo", "cdc", "domainMetadata")
      val deltaAuxColumns =
        Set("version", "inCommitTimestamp", "add_path_canonical", "remove_path_canonical")

      val structActionColumns = plan.output.collect {
        case attr
          if deltaActionStructColumns.contains(attr.name) &&
            attr.dataType.isInstanceOf[StructType] =>
          attr.name
      }.toSet
      val auxColumns = plan.output.map(_.name).toSet.intersect(deltaAuxColumns)

      structActionColumns.size >= 2 && auxColumns.nonEmpty
    }
  }

  private def intercept(delegate: Rule[SparkPlan]): Rule[SparkPlan] = {
    new OmniSparkRuleInterceptor(delegate)
  }

}
