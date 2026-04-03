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
package org.apache.gluten.execution

import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}

import scala.collection.mutable
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

/**
 * Hudi base-file scan for Omni: uses [[OmniFileSourceScanTransformHelper]] so Substrait ReadRel
 * carries Parquet enhancement JSON. [[HudiScanTransformer]] inherits
 * [[BasicScanExecTransformer]]'s read node only and leaves native enhancement empty, which makes
 * `ParseEnhanceJson` fail on an empty input.
 */
case class OmniHudiScanTransformer(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanExecTransformerBase(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  /**
   * Spark's [[metadataColumns]] only collects FileSource* metadata attributes; Hudi exposes
   * `_hoodie_*` as plain [[AttributeReference]] in [[output]]. Without listing them here,
   * [[BasicScanExecTransformer.getSplitInfosFromPartitions]] passes no `_hoodie_` names to native
   * split info and [[org.apache.gluten.backendsapi.omni.HudiSplitMetadataColumns]] never runs.
   */
  override def getMetadataColumns(): Seq[AttributeReference] = {
    val fromSpark = metadataColumns
    val fromSparkNames = fromSpark.map(_.name).toSet
    val hoodieExtra = {
      val buf = mutable.ArrayBuffer[AttributeReference]()
      val seen = mutable.Set[String]()
      output.foreach {
        case ar: AttributeReference
            if ar.name.startsWith("_hoodie_") &&
              !fromSparkNames.contains(ar.name) &&
              seen.add(ar.name) =>
          buf += ar
        case _ =>
      }
      buf.toSeq
    }
    fromSpark ++ hoodieExtra
  }

  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  override protected def doValidateInternal(): ValidationResult = {
    super.doValidateInternal()
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    OmniFileSourceScanTransformHelper.doTransform(this, context, ReadFileFormat.ParquetReadFormat)
  }

  override def doCanonicalize(): OmniHudiScanTransformer = {
    OmniHudiScanTransformer(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan
    )
  }
}

object OmniHudiScanTransformer {

  def apply(scanExec: FileSourceScanExec): OmniHudiScanTransformer =
    OmniHudiScanTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
}
