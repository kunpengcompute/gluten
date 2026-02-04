package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, OmniInsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.hive.execution.{InsertIntoHiveTable, OmniInsertIntoHiveTable}

case class OffloadWrite() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case d@DataWritingCommandExec(cmd, child) if !child.isInstanceOf[WriteFilesExec] =>
      cmd match {
        case hive: InsertIntoHiveTable =>
          d.copy(OmniInsertIntoHiveTable.apply(
            hive.table,
            hive.partition,
            hive.query,
            hive.overwrite,
            hive.ifPartitionNotExists,
            hive.outputColumnNames
          ))
        case hadoop: InsertIntoHadoopFsRelationCommand =>
          d.copy(OmniInsertIntoHadoopFsRelationCommand(
            hadoop.outputPath,
            hadoop.staticPartitions,
            hadoop.ifPartitionNotExists,
            hadoop.partitionColumns,
            hadoop.bucketSpec,
            hadoop.fileFormat,
            hadoop.options,
            hadoop.query,
            hadoop.mode,
            hadoop.catalogTable,
            hadoop.fileIndex,
            hadoop.outputColumnNames
          ))
        case _ =>
          d
      }
    case other => other
  }
}
