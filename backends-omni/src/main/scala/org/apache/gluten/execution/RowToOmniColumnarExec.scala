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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.SparkMemoryUtils
import org.apache.gluten.vectorized.OmniColumnVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer


case class RowToOmniColumnarExec(child: SparkPlan) extends RowToColumnarExecBase(child = child) {
  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val enableOffHeapColumnVector = SQLConf.get.offHeapColumnVectorEnabled
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val rowToOmniColumnarTime = longMetric("rowToOmniColumnarTime")
    val numRows = GlutenConfig.get.maxBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions { rowIterator =>
      InternalRowToColumnarBatch.convert(enableOffHeapColumnVector, numInputRows, numOutputBatches, rowToOmniColumnarTime, numRows, localSchema, rowIterator)
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowToOmniColumnarExec =
    copy(child = newChild)
}

object InternalRowToColumnarBatch {
  final val NANOSECONDS  = java.util.concurrent.TimeUnit.NANOSECONDS
  def convert(enableOffHeapColumnVector: Boolean,
              numInputRows: SQLMetric,
              numOutputBatches: SQLMetric,
              rowToOmniColumnarTime: SQLMetric,
              numRows: Int, localSchema: StructType,
              rowIterator: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      new Iterator[ColumnarBatch] {
        private val converters = new OmniRowToColumnConverter(localSchema)

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          val startTime = System.nanoTime()

          var rowCount = 0
          val bufferRow = ListBuffer[InternalRow]()
          while (rowCount < numRows && rowIterator.hasNext) {
            var row = rowIterator.next()
            bufferRow += row.copy()
            rowCount += 1
          }

          val vectors = converters.convert(bufferRow, rowCount)
          val cb: ColumnarBatch = new ColumnarBatch(vectors.toArray)
          cb.setNumRows(rowCount)
          numInputRows += rowCount
          numOutputBatches += 1
          rowToOmniColumnarTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          cb
        }
      }
    } else {
      Iterator.empty
    }
  }
}

private[execution] class OmniRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => OmniRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(rows: Seq[InternalRow], size: Int): Seq[WritableColumnVector] = {
    var idx = 0
    val res = new Array[WritableColumnVector](schema.fields.length)
    while (idx < schema.fields.length) {
      res(idx) = converters(idx).add(rows, idx, size)
      idx += 1
    }
    res.toSeq
  }
}

/**
 * Provides an optimized set of APIs to extract a column from a row and append it to a
 * [[WritableColumnVector]].
 */
private object OmniRowToColumnConverter {
  SparkMemoryUtils.init()

  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit
    def add(rows: Seq[SpecializedGetters], column: Int, size: Int): WritableColumnVector
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    val core = dataType match {
      case BinaryType => BinaryConverter
      case BooleanType | NullType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType | DateType => IntConverter(dataType)
      case LongType | TimestampType => LongConverter(dataType)
      case DoubleType => DoubleConverter
      case StringType => StringConverter
      case CalendarIntervalType => CalendarConverter
      case dt: DecimalType => DecimalConverter(dt)
      case struct: StructType =>
        val fieldConverters = struct.fields.map { f =>
          getConverterForType(f.dataType, f.nullable)
        }
        StructConverter(fieldConverters, struct)
      case map: MapType =>
        val keyConverter = getConverterForType(map.keyType, nullable)
        val valueConverter = getConverterForType(map.valueType, map.valueContainsNull)
        MapConverter(keyConverter, valueConverter, map)
      case unknown => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
    core
  }

  private case class MapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter,
      dataType: MapType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      throw new UnsupportedOperationException("StructConverter not support append()")
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, dataType, true)
      // count total offset
      var totalLen = 0
      val offsets = new ListBuffer[Int]
      val nulls = new ListBuffer[Byte]
      offsets += 0
      for (row <- rows) {
        val mapData = if (row == null) null else row.getMap(column)
        if (mapData == null) {
          nulls += 1
        } else {
          nulls += 0
          val num = mapData.numElements
          totalLen += num
        }
        offsets += totalLen
      }

      val keyVector = new OmniColumnVector(totalLen, dataType.keyType, true)
      val valueVector = new OmniColumnVector(totalLen, dataType.valueType, true)
      for (row <- rows) {
        val mapData = if (row == null) null else row.getMap(column)
        if (mapData != null) {
          val mapLength = mapData.numElements
          for (i <- 0 until mapLength) {
            keyConverter.append(mapData.keyArray(), i, keyVector)
            valueConverter.append(mapData.valueArray(), i, valueVector)
          }
        }
      }

      cv.setChild(keyVector, 0)
      cv.setChild(valueVector, 1)
      cv.setOffsets(offsets.toArray)
      cv.updateVec()
      cv.putNulls(0, nulls.toArray, size)
      cv
    }
  }

  private case class StructConverter(childConverters: Array[TypeConverter], dataType: StructType)
      extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      throw new UnsupportedOperationException("StructConverter not support append()")
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      // not init child
      val cv = new OmniColumnVector(size, dataType, true)
      val structRows = new ListBuffer[SpecializedGetters]()
      val nulls = new ListBuffer[Byte]()
      for (row <- rows) {
        val struct = if (row == null) null else row.getStruct(column, childConverters.length)
        if (struct == null) {
          nulls += 1
        } else {
          nulls += 0
        }
        structRows += struct
      }
      childConverters.zipWithIndex.foreach { case (childConverter, fieldIndex) =>
        val vector = childConverter.add(structRows, fieldIndex, size).asInstanceOf[OmniColumnVector]
        cv.setChild(vector, fieldIndex)
      }
      cv.updateVec()
      cv.putNulls(0, nulls.toArray, size)
      cv
    }
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        val bytes = row.getBinary(column)
        cv.appendByteArray(bytes, 0, bytes.length)
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, BinaryType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendBoolean(row.getBoolean(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, BooleanType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendByte(row.getByte(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, ByteType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendShort(row.getShort(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, ShortType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private case class IntConverter(dataType: DataType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendInt(row.getInt(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, dataType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private case class LongConverter(dataType: DataType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendLong(row.getLong(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, dataType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendDouble(row.getDouble(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, DoubleType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        val data = row.getUTF8String(column).getBytes
        cv.asInstanceOf[OmniColumnVector].appendString(data.length, data, 0)
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, StringType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private object CalendarConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        val c = row.getInterval(column)
        cv.appendStruct(false)
        cv.getChild(0).appendInt(c.months)
        cv.getChild(1).appendInt(c.days)
        cv.getChild(2).appendLong(c.microseconds)
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, CalendarIntervalType, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }

  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        val d = row.getDecimal(column, dt.precision, dt.scale)
        if (DecimalType.is64BitDecimalType(dt)) {
          cv.appendLong(d.toUnscaledLong)
        } else {
          cv.asInstanceOf[OmniColumnVector].appendDecimal(d)
        }
      }
    }

    override def add(rows: Seq[SpecializedGetters],
        column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, dt, true)
      for (row <- rows) {
        if (row == null || row.isNullAt(column)) {
          cv.appendNull
        } else {
          append(row, column, cv)
        }
      }
      cv
    }
  }
}
