/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter
import org.apache.gluten.execution.DeltaNativeParquetWrite
import org.apache.gluten.expression.OmniExpressionAdaptor.perBatchColumnOmniTypeIds
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{FakeRow, OutputWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.Array.{emptyBooleanArray, emptyIntArray}

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
class OmniParquetOutputWriter(path: String, dataSchema: StructType,
                              context: TaskAttemptContext)
  extends OutputWriter {
  private val datetimeRebaseMode = SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE)

  val writer = new ParquetColumnarBatchWriter(datetimeRebaseMode == "LEGACY")
  var dataColumnsIds: Array[Boolean] = emptyBooleanArray
  /** Same contract as Iceberg write: ids per `ColumnarBatch` column (here includes partition cols). */
  var batchColumnOmniTypeIds: Array[Int] = emptyIntArray

  /** When true (Delta transactional Parquet only), JNI uses Iceberg-style write after projecting batch. */
  private var projectDataColumnsOnly: Boolean = false
  private var dataColumnIndicesInBatch: Array[Int] = _
  private var fileOmniTypeIds: Array[Int] = _

  def initialize(allColumns: Seq[Attribute], dataColumns: Seq[Attribute]): Unit = {
    val filePath = new Path(path)
    writer.initializeSchemaJava(dataSchema)
    writer.initializeWriterJava(filePath)
    projectDataColumnsOnly =
      context != null &&
        context.getConfiguration.getBoolean(DeltaNativeParquetWrite.JOB_CONF_PROJECT_DATA_COLUMNS, false)
    if (projectDataColumnsOnly) {
      fileOmniTypeIds = perBatchColumnOmniTypeIds(dataSchema)
      val colsSeq = allColumns.toIndexedSeq
      dataColumnIndicesInBatch = dataColumns.map { dc =>
        val idx = colsSeq.indexWhere(_.exprId == dc.exprId)
        if (idx < 0) {
          throw new IllegalStateException(
            s"OmniParquetOutputWriter (Delta projection): data column ${dc.name} not in output order")
        }
        idx
      }.toArray
      if (dataColumnIndicesInBatch.length != dataSchema.length) {
        throw new IllegalStateException(
          "OmniParquetOutputWriter (Delta projection): data column count != dataSchema length")
      }
    } else {
      batchColumnOmniTypeIds = perBatchColumnOmniTypeIds(allColumns.toStructType)
      dataColumnsIds = allColumns.map(x => dataColumns.contains(x)).toArray
    }
  }

  override def write(row: InternalRow): Unit = {
    assert(row.isInstanceOf[FakeRow])
    val batch = row.asInstanceOf[FakeRow].batch
    if (projectDataColumnsOnly) {
      val projected = DeltaNativeParquetWrite.projectDataColumns(batch, dataColumnIndicesInBatch)
      DeltaNativeParquetWrite.writeLikeIceberg(writer, fileOmniTypeIds, projected)
    } else {
      writer.write(batchColumnOmniTypeIds, dataColumnsIds, batch)
    }
  }

  def spiltWrite(row: InternalRow, startPos: Long, endPos: Long): Unit = {
    assert(row.isInstanceOf[FakeRow])
    val batch = row.asInstanceOf[FakeRow].batch
    if (projectDataColumnsOnly) {
      val projected = DeltaNativeParquetWrite.projectDataColumns(batch, dataColumnIndicesInBatch)
      DeltaNativeParquetWrite.splitWriteLikeIceberg(writer, fileOmniTypeIds, projected, startPos, endPos)
    } else {
      writer.splitWrite(batchColumnOmniTypeIds, batchColumnOmniTypeIds, dataColumnsIds,
        batch, startPos, endPos)
    }
  }

  override def close(): Unit = {
    writer.close()
  }

  override def path(): String = {
    path
  }
}
