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
package org.apache.spark.sql.delta

import java.util.Locale

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.FileDelayedCommitProtocol
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{
  BasicWriteJobStatsTracker,
  DeltaV1Writes,
  OmniFileFormatWriter,
  OmniGlutenWriterColumnarRules,
  WriteJobStatsTracker
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.util.SerializableConfiguration

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.ListBuffer

class OmniOptimisticTransaction(
    override val deltaLog: DeltaLog,
    override val catalogTable: Option[CatalogTable],
    override val snapshot: Snapshot)
  extends OptimisticTransaction(deltaLog, catalogTable, snapshot) {

  def this(
      deltaLog: DeltaLog,
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None) {
    this(
      deltaLog,
      catalogTableOpt,
      snapshotOpt.getOrElse(deltaLog.update())
    )
  }

  private def nativeFormatName(fileFormat: org.apache.spark.sql.execution.datasources.FileFormat): String = {
    fileFormat match {
      case register: DataSourceRegister => register.shortName()
      case other => other.getClass.getSimpleName.stripSuffix("FileFormat").toLowerCase(Locale.ROOT)
    }
  }

  private def nativeWriterEnabled: Boolean = {
    GlutenConfig.get.enableNativeWriter.getOrElse(
      SparkShimLoader.getSparkShims.enableNativeWriteFilesByDefault())
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    if (!nativeWriterEnabled) {
      logWarning(
        s"[Omni-Proof][DeltaWrite] native Delta transactional write disabled; " +
          s"falling back to Delta default writeFiles for path=${deltaLog.dataPath}")
      return super.writeFiles(inputData, writeOptions, isOptimize, additionalConstraints)
    }

    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _) =
      normalizeData(deltaLog, writeOptions, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = new FileDelayedCommitProtocol("delta", outputPath.toString, None, None)

    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      logWarning(
        s"[Omni-Proof][DeltaWrite] disabling Delta per-row stats tracker for native write; " +
          s"FileFormatWriter stats callbacks currently receive FakeRow batches for path=$outputPath")
    }
    val optionalStatsTrackers: Seq[WriteJobStatsTracker] = Seq.empty

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    val rowShuffleConfKey = "spark.gluten.sql.columnar.backend.omni.rowShuffle.enabled"
    val previousRowShuffle = spark.conf.getOption(rowShuffleConfKey)
    val fileFormat = deltaLog.fileFormat(protocol, metadata)
    val formatName = nativeFormatName(fileFormat)

    logWarning(
      s"[Omni-Proof][DeltaWrite] preparing native Delta transactional write; " +
        s"path=$outputPath, format=$formatName, " +
        s"partitionColumns=${partitioningColumns.map(_.name).mkString("[", ",", "]")}, " +
        s"output=${output.map(_.name).mkString("[", ",", "]")}, optimize=$isOptimize")

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = OmniFileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(deltaWriteOptions) =>
          deltaWriteOptions.options.filter {
            case (key, _) =>
              key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }
      }

      val executedPlan = DeltaV1Writes(
        spark,
        queryExecution.executedPlan,
        fileFormat,
        partitioningColumns,
        None,
        options
      ).executedPlan

      logWarning(
        s"[Omni-Proof][DeltaWrite] DeltaV1Writes produced native candidate plan; " +
          s"plan=${executedPlan.nodeName}, format=$formatName, " +
          s"output=${executedPlan.output.map(_.name).mkString("[", ",", "]")}")

      try {
        spark.conf.set(rowShuffleConfKey, "false")
        OmniGlutenWriterColumnarRules.injectSparkLocalProperty(spark, Some(formatName))
        logWarning(
          s"[Omni-Proof][DeltaWrite] invoking OmniFileFormatWriter.write for Delta transactional " +
            s"write; path=$outputPath, format=$formatName, " +
            s"nativeFormat=${spark.sparkContext.getLocalProperty("nativeFormat")}")
        OmniFileFormatWriter.write(
          sparkSession = spark,
          plan = executedPlan,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = outputSpec,
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTrackers ++ statsTrackers,
          options = options
        )
      } catch {
        case s: SparkException =>
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      } finally {
        previousRowShuffle match {
          case Some(value) => spark.conf.set(rowShuffleConfKey, value)
          case None => spark.conf.unset(rowShuffleConfKey)
        }
        OmniGlutenWriterColumnarRules.injectSparkLocalProperty(spark, None)
        logWarning(
          s"[Omni-Proof][DeltaWrite] Delta transactional write finished; " +
            s"addedFiles=${committer.addedStatuses.size}, changeFiles=${committer.changeFiles.size}, " +
            s"path=$outputPath")
      }
    }

    committer.addedStatuses.toSeq ++ committer.changeFiles
  }
}
