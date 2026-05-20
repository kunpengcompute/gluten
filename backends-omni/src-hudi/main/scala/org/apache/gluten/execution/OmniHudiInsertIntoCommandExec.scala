/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.gluten.connector.write.OmniHudiDataWriteFactory
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform

import org.apache.hudi.{AvroConversionUtils, DataSourceUtils}
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.DataSourceInternalWriterHelper
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.datasources.v2.{DataWritingColumnarBatchSparkTask, DataWritingColumnarBatchSparkTaskResult}
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util

import scala.collection.JavaConverters._

/**
 * Executes Hudi SQL INSERT through the Omni columnar write path.
 *
 * Hudi's Spark SQL extension plans INSERT as [[InsertIntoHoodieTableCommand]] instead of DSv2
 * [[org.apache.spark.sql.execution.datasources.v2.AppendDataExec]], so the normal Append offload
 * rule cannot see it. This node bridges that command path to the same Omni Hudi columnar writer
 * ([[OmniHudiDataWriteFactory]]) used by DSv2 append, then delegates timeline commit to Hudi's
 * [[DataSourceInternalWriterHelper]].
 *
 * @since 2026
 */
case class OmniHudiInsertIntoCommandExec(command: InsertIntoHoodieTableCommand)
  extends LeafV2CommandExec {

  override def nodeName: String = "OmniHudiInsertIntoCommand"

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    OmniHudiInsertIntoCommandExec.run(command)
    Nil
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new IllegalArgumentException("OmniHudiInsertIntoCommandExec is a leaf node")
    }
    this
  }
}

object OmniHudiInsertIntoCommandExec extends ProvidesHoodieConfig {

  private val hudiLog = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
   * INSERT entry point: append only; overwrite falls back to native [[InsertIntoHoodieTableCommand.run]].
   */
  def run(command: InsertIntoHoodieTableCommand): Seq[Row] = {
    val sparkSession = SparkSession.active
    if (command.overwrite) {
      return command.run(sparkSession)
    }

    require(command.logicalRelation.catalogTable.isDefined, "Missing catalog table")
    val table = command.logicalRelation.catalogTable.get
    val catalogTable = new HoodieCatalogTable(sparkSession, table)
    val (mode, isOverwriteTable, isOverwritePartition, staticOverwritePartitionPathOpt) =
      (SaveMode.Append, false, false, scala.Option.empty[String])

    val config = buildHoodieInsertConfig(
      catalogTable,
      sparkSession,
      isOverwritePartition,
      isOverwriteTable,
      command.partitionSpec,
      Map.empty,
      staticOverwritePartitionPathOpt)

    // Align query output to table schema (including static partition columns), same as native command
    val alignedQuery = alignQueryOutput(
      command.query,
      catalogTable,
      command.partitionSpec,
      sparkSession.sessionState.conf)

    hudiLog.warn(
      "[OmniHudi][command-offload] InsertIntoHoodieTableCommand -> OmniHudiInsertIntoCommandExec; " +
        "table=" + table.identifier.unquotedString + " basePath=" + catalogTable.tableLocation +
        " mode=" + mode.name())

    writeAndCommit(sparkSession, table.identifier.unquotedString, catalogTable, alignedQuery, config)
    sparkSession.catalog.refreshTable(table.identifier.unquotedString)
    Seq.empty[Row]
  }

  /**
   * Driver: create inflight commit → run columnar write tasks → collect WriteStatus per task → commit timeline.
   */
  private def writeAndCommit(
      sparkSession: SparkSession,
      tableName: String,
      catalogTable: HoodieCatalogTable,
      alignedQuery: LogicalPlan,
      config: Map[String, String]): Unit = {
    val sourceSchema = alignedQuery.schema
    val rawPlan = sparkSession.sessionState.executePlan(alignedQuery, CommandExecutionMode.SKIP).executedPlan
    // Apply columnar heuristic transform (consistent with other Gluten operators)
    val columnarPlan = HeuristicTransform.static().apply(rawPlan)
    val basePath = catalogTable.tableLocation
    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val (recordName, recordNamespace) =
      AvroConversionUtils.getAvroRecordNameAndNamespace(catalogTable.tableName)
    val writerSchema =
      AvroConversionUtils.convertStructTypeToAvroSchema(sourceSchema, recordName, recordNamespace)
    val writeConfig = DataSourceUtils.createHoodieConfig(
      writerSchema.toString,
      basePath,
      catalogTable.tableName,
      config.asJava).asInstanceOf[HoodieWriteConfig]
    val helper = new DataSourceInternalWriterHelper(
      instantTime,
      writeConfig,
      sourceSchema,
      sparkSession,
      HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf),
      DataSourceUtils.getExtraMetadata(config.asJava))

    helper.createInflightCommit()
    val messages = runColumnarWriteJob(
      sparkSession,
      columnarPlan,
      config,
      basePath,
      instantTime,
      catalogTable.partitionSchema.fieldNames)
    val writeStatuses = new util.ArrayList[WriteStatus]()
    messages.foreach { message =>
      helper.onDataWriterCommit(String.valueOf(message))
      // Collect WriteStatus list from OmniHudiWriterCommitMessage (or Hudi-native message types)
      extractWriteStatuses(message).foreach(writeStatuses.add)
    }

    try {
      helper.commit(writeStatuses)
    } catch {
      case t: Throwable =>
        helper.abort()
        throw t
    }
  }

  /**
   * Columnar write per Spark partition via [[OmniHudiDataWriteFactory]] and [[HudiWriteJniWrapper]];
   * each task returns a [[WriterCommitMessage]] carrying WriteStatus instances.
   */
  private def runColumnarWriteJob(
      sparkSession: SparkSession,
      query: SparkPlan,
      config: Map[String, String],
      basePath: String,
      instantTime: String,
      partitionColumns: Seq[String]): Array[WriterCommitMessage] = {
    val writeInputPlan =
      if (query.supportsColumnar) {
        query
      } else {
        RowToOmniColumnarExec(query)
      }
    val rdd: RDD[ColumnarBatch] = {
      val out = writeInputPlan.executeColumnar()
      // Empty input: still run one empty partition so runJob has a partition for commit metadata
      if (out.partitions.isEmpty) {
        sparkSession.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        out
      }
    }
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val factory = OmniHudiDataWriteFactory(
      writeInputPlan.schema,
      basePath,
      config.getOrElse("hoodie.parquet.compression.codec", "snappy"),
      config
        .get("hoodie.datasource.write.file.format")
        .orElse(config.get("hoodie.table.base.file.format"))
        .getOrElse("parquet"),
      instantTime,
      partitionColumns,
      recordKeyColumns(config))

    try {
      sparkSession.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          DataWritingColumnarBatchSparkTask.run(factory, context, iter, Map.empty),
        rdd.partitions.indices,
        (index, result: DataWritingColumnarBatchSparkTaskResult) => {
          messages(index) = result.writerCommitMessage
        })
      messages
    } catch {
      case t: Throwable =>
        throw new SparkException("Omni Hudi INSERT columnar write job failed", t)
    }
  }

  /** Reflectively invoke getWriteStatuses() on commit message (Omni or Hudi-native types). */
  private def extractWriteStatuses(message: WriterCommitMessage): Seq[WriteStatus] = {
    if (message == null) {
      return Nil
    }
    try {
      val method = message.getClass.getMethod("getWriteStatuses")
      method.invoke(message).asInstanceOf[java.util.List[WriteStatus]].asScala.toSeq
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          "Hudi WriterCommitMessage does not expose getWriteStatuses; cannot commit Omni Hudi INSERT",
          e)
    }
  }

  /** Parse record key column names from Hoodie write config (comma-separated for composite keys). */
  private def recordKeyColumns(config: Map[String, String]): Seq[String] = {
    config
      .get("hoodie.datasource.write.recordkey.field")
      .orElse(config.get("hoodie.datasource.write.recordkey.fields"))
      .toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  /**
   * Align INSERT subquery output to table schema: column/type matching, static partition literals, validation.
   * Mirrors native InsertIntoHoodieTableCommand column alignment.
   */
  private def alignQueryOutput(
      query: LogicalPlan,
      catalogTable: HoodieCatalogTable,
      partitionsSpec: Map[String, Option[String]],
      conf: SQLConf): LogicalPlan = {
    val targetPartitionSchema = catalogTable.partitionSchema
    val staticPartitionValues = filterStaticPartitionValues(partitionsSpec)
    val cleanedQuery = query
    val expectedQueryColumns =
      catalogTable.tableSchemaWithoutMetaFields.filterNot(f => staticPartitionValues.contains(f.name))
    val coercedQueryOutput =
      coerceQueryOutputColumns(StructType(expectedQueryColumns), cleanedQuery, catalogTable, conf)
    validate(removeMetaFields(coercedQueryOutput.schema), partitionsSpec, catalogTable)
    val staticPartitionValuesExprs =
      createStaticPartitionValuesExpressions(staticPartitionValues, targetPartitionSchema, conf)
    Project(coercedQueryOutput.output ++ staticPartitionValuesExprs, coercedQueryOutput)
  }

  /** Resolve output columns by name first; fall back to position (Spark write-table behavior). */
  private def coerceQueryOutputColumns(
      expectedSchema: StructType,
      query: LogicalPlan,
      catalogTable: HoodieCatalogTable,
      conf: SQLConf): LogicalPlan = {
    val planUtils = sparkAdapter.getCatalystPlanUtils
    try {
      planUtils.resolveOutputColumns(
        catalogTable.catalogTableName,
        sparkAdapter.getSchemaUtils.toAttributes(expectedSchema),
        query,
        byName = true,
        conf)
    } catch {
      case ae: AnalysisException
          if ae.getMessage.startsWith(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the table") ||
            ae.getMessage.startsWith("Cannot write incompatible data to table") =>
        planUtils.resolveOutputColumns(
          catalogTable.catalogTableName,
          sparkAdapter.getSchemaUtils.toAttributes(expectedSchema),
          query,
          byName = false,
          conf)
    }
  }

  /** Validate partition spec vs table partition columns and castability of query output to table schema. */
  private def validate(
      queryOutputSchema: StructType,
      partitionsSpec: Map[String, Option[String]],
      catalogTable: HoodieCatalogTable): Unit = {
    if (partitionsSpec.nonEmpty && partitionsSpec.size != catalogTable.partitionSchema.size) {
      throw new IllegalArgumentException(
        s"Required partition schema is: ${catalogTable.partitionSchema.fieldNames.mkString("[", ", ", "]")}, " +
          s"partition spec is: ${partitionsSpec.mkString("[", ", ", "]")}")
    }
    val staticPartitionValues = filterStaticPartitionValues(partitionsSpec)
    val fullQueryOutputSchema =
      StructType(queryOutputSchema.fields ++ staticPartitionValues.keys.map(StructField(_, StringType)))
    if (!conforms(fullQueryOutputSchema, catalogTable.tableSchemaWithoutMetaFields)) {
      throw new IllegalArgumentException(
        s"Expected table's schema: ${catalogTable.tableSchemaWithoutMetaFields.fields.mkString("[", ", ", "]")}, " +
          s"query's output (including static partition values): ${fullQueryOutputSchema.fields.mkString("[", ", ", "]")}")
    }
  }

  /** Literal projections for static partition columns in INSERT (e.g. dt = '2026-05-01'). */
  private def createStaticPartitionValuesExpressions(
      staticPartitionValues: Map[String, String],
      partitionSchema: StructType,
      conf: SQLConf): Seq[NamedExpression] = {
    partitionSchema.fields
      .filter(pf => staticPartitionValues.contains(pf.name))
      .map { pf =>
        val staticPartitionValue = staticPartitionValues(pf.name)
        val castExpr = castIfNeeded(Literal.create(staticPartitionValue), pf.dataType)
        Alias(castExpr, pf.name)()
      }
  }

  private def conforms(sourceSchema: StructType, targetSchema: StructType): Boolean = {
    sourceSchema.fields.length == targetSchema.fields.length &&
    targetSchema.fields.zip(sourceSchema).forall {
      case (targetColumn, sourceColumn) =>
        Cast.canCast(sourceColumn.dataType, targetColumn.dataType)
    }
  }

  /** Static partition values from partition spec (entries where Option is Some). */
  private def filterStaticPartitionValues(partitionsSpec: Map[String, Option[String]]): Map[String, String] =
    partitionsSpec.filter(_._2.isDefined).mapValues(_.get).toMap
}
