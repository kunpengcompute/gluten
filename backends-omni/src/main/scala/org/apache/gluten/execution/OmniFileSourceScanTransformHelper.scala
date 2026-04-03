/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.protobuf.StringValue
import com.huawei.boostkit.spark.jni.{OrcPushFilterBuilder, ParquetPushFilterBuilder}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder

import io.substrait.proto.NamedStruct
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

import scala.collection.JavaConverters._

/** Shared Substrait read mapping for Omni native file scans (Parquet/ORC pushdown JSON). */
private[execution] object OmniFileSourceScanTransformHelper {

  def doTransform(
      scan: FileSourceScanExecTransformerBase,
      context: SubstraitContext,
      substraitReadFormat: ReadFileFormat): TransformContext = {
    val output = scan.outputAttributes()
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val hoodiePrefixAsMetadata =
      HudiDatasourceDetection.isHudiSparkFileFormat(scan.relation.fileFormat)
    val columnTypeNodes = output.map {
      attr =>
        if (scan.getPartitionSchema.exists(_.name.equals(attr.name))) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else if (SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name)) {
          new ColumnTypeNode(NamedStruct.ColumnType.ROWINDEX_COL)
        } else if (
          scan.isMetadataColumn(attr) ||
          (hoodiePrefixAsMetadata && attr.name.startsWith("_hoodie_"))) {
          new ColumnTypeNode(NamedStruct.ColumnType.METADATA_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava
    val transformer = scan
      .filterExprs()
      .map(ExpressionConverter.replaceAttributeReference)
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    val optimizationContent =
      s"isMergeTree=${if (substraitReadFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"}\n"

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(optimizationContent).build)
    val filter =
      scan.omniPushedDownDataSourceFilters.reduceOption(org.apache.spark.sql.sources.And(_, _))

    val rawJson = substraitReadFormat match {
      case ReadFileFormat.OrcReadFormat =>
        val orcBuilder =
          new OrcPushFilterBuilder(scan.relation.dataSchema, scan.requiredSchema)
        orcBuilder.buildPushFilterJson(
          filter.orNull,
          scan.session.sessionState.conf.orcFilterPushDown,
          scan.session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER))

      case ReadFileFormat.ParquetReadFormat =>
        val datetimeRebaseModeStr =
          scan.session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
        val int96RebaseModeStr =
          scan.session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)

        def toLegacyBehaviorPolicy(modeStr: String): LegacyBehaviorPolicy.Value = {
          modeStr match {
            case "LEGACY" => LegacyBehaviorPolicy.LEGACY
            case "CORRECTED" => LegacyBehaviorPolicy.CORRECTED
            case "EXCEPTION" => LegacyBehaviorPolicy.EXCEPTION
            case _ => LegacyBehaviorPolicy.LEGACY
          }
        }

        val datetimeRebaseMode = toLegacyBehaviorPolicy(datetimeRebaseModeStr)
        val int96RebaseMode = toLegacyBehaviorPolicy(int96RebaseModeStr)

        val datetimeRebaseSpec = new RebaseDateTime.RebaseSpec(datetimeRebaseMode, scala.None)
        val int96RebaseSpec = new RebaseDateTime.RebaseSpec(int96RebaseMode, scala.None)

        val parquetBuilder = new ParquetPushFilterBuilder(
          scan.relation.dataSchema,
          scan.requiredSchema,
          datetimeRebaseSpec,
          int96RebaseSpec)
        parquetBuilder.buildPushFilterJson(
          filter.orNull,
          scan.session.sessionState.conf.parquetFilterPushDown,
          scan.session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER))

      case _ => "{}"
    }
    val json = Option(rawJson).map(_.trim).filter(_.nonEmpty).getOrElse("{}")

    val extraProto = BackendsApiManager.getTransformerApiInstance.packPBMessage(
      StringValue.newBuilder.setValue(json).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, extraProto)

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      exprNode,
      extensionNode,
      context,
      context.nextOperatorId(scan.nodeName))
    TransformContext(output, readNode)
  }
}
