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

package org.apache.gluten.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce._
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetUtils, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf

class OmniParquetFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  var canVecPredicateFilter = false

  def setVecPredicateFilter(): Unit = {
    canVecPredicateFilter = true
  }

  override def shortName(): String = "parquet-native"

  override def toString: String = "PARQUET-NATIVE"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OmniParquetFileFormat]

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sparkSession.sessionState.conf.parquetOutputTimestampType.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
      && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
      && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
        s" create job summaries. " +
        s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new OmniParquetOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            parameters: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  override def buildReaderWithPartitionValues(
                                               sparkSession: SparkSession,
                                               dataSchema: StructType,
                                               partitionSchema: StructType,
                                               requiredSchema: StructType,
                                               filters: Seq[Filter],
                                               options: Map[String, String],
                                               hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    throw new UnsupportedOperationException("buildReaderWithPartitionValues should not be called")
  }
}