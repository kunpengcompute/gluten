/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.connector.write

import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.iceberg._
import org.apache.iceberg.spark.source.IcebergWriteUtil

/**
 * Omni Iceberg columnar DataWriter: holds jniWrapper. write() passes ColumnarBatch to
 * IcebergWriteJniWrapper (which calls Parquet/ORC writer.write). commit() gets JSON array from
 * jniWrapper.commit, parses to DataFile, then IcebergWriteUtil.commitDataFiles.
 */
case class OmniIcebergColumnarBatchDataWriter(
    jniWrapper: IcebergWriteJniWrapper,
    format: Int,
    partitionSpec: PartitionSpec,
    sortOrder: SortOrder)
  extends DataWriter[ColumnarBatch]
  with Logging {

  private val mapper = {
    val m = new ObjectMapper()
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  override def write(batch: ColumnarBatch): Unit = {
    jniWrapper.write(batch)
  }

  override def commit(): WriterCommitMessage = {
    val dataFiles = jniWrapper.commit().map(d => parseDataFile(d, partitionSpec, sortOrder))
    IcebergWriteUtil.commitDataFiles(dataFiles)
  }

  override def abort(): Unit = {
    logInfo("Abort OmniIcebergColumnarBatchDataWriter")
  }

  override def close(): Unit = {
    logDebug("Close OmniIcebergColumnarBatchDataWriter")
  }

  private def parseDataFile(json: String, spec: PartitionSpec, sortOrder: SortOrder): DataFile = {
    val dataFile = mapper.readValue(json, classOf[DataFileJson])
    val builder = DataFiles
      .builder(spec)
      .withPath(dataFile.getPath)
      .withFormat(getFileFormat)
      .withFileSizeInBytes(dataFile.getFileSizeInBytes)
    if (dataFile.getPartitionDataJson != null) {
      PartitionDataJson.fromJson(dataFile.getPartitionDataJson, spec).ifPresent(pd => builder.withPartition(pd))
    }
    builder
      .withMetrics(dataFile.getMetrics.metrics())
      .withSplitOffsets(dataFile.getSplitOffsets)
      .withSortOrder(sortOrder)
      .build()
  }

  private def getFileFormat: FileFormat = {
    format match {
      case 0 => FileFormat.ORC
      case 1 => FileFormat.PARQUET
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val m = jniWrapper.metrics()
    if (m == null) Array.empty[CustomTaskMetric]
    else m.toCustomTaskMetrics
  }
}
