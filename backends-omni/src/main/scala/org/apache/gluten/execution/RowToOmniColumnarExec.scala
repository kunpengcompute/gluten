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
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

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
      case FloatType => FloatConverter
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
      case array: ArrayType =>
        val elementConverter = getConverterForType(array.elementType, array.containsNull)
        ArrayConverter(elementConverter, array)
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
      throw new UnsupportedOperationException("MapConverter not support append()")
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

      val mapKeys = new ListBuffer[SpecializedGetters]()
      val mapValues = new ListBuffer[SpecializedGetters]()
      for (row <- rows) {
        val mapData = if (row == null) null else row.getMap(column)
        if (mapData != null) {
          val mapLength = mapData.numElements
          for (i <- 0 until mapLength) {
            mapKeys += new ArrayElementGetter(mapData.keyArray(), i)
            mapValues += new ArrayElementGetter(mapData.valueArray(), i)
          }
        }
      }
      val keyVector = keyConverter.add(mapKeys.toSeq, 0, totalLen)
      val valueVector = valueConverter.add(mapValues.toSeq, 0, totalLen)

      cv.setChild(keyVector, 0)
      cv.setChild(valueVector, 1)
      cv.setOffsets(offsets.toArray)
      cv.updateVec()
      cv.putNulls(0, nulls.toArray, size)
      cv
    }
  }

  private case class ArrayConverter(
      elementConverter: TypeConverter,
      dataType: ArrayType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      throw new UnsupportedOperationException("ArrayConverter not support append()")
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
        val arrayData = if (row == null) null else row.getArray(column)
        if (arrayData == null) {
          nulls += 1
        } else {
          nulls += 0
          val num = arrayData.numElements
          totalLen += num
        }
        offsets += totalLen
      }

      val elementVector: WritableColumnVector = dataType.elementType match {
        case innerArrayType: ArrayType =>
          buildNestedArrayElementVector(rows, column, totalLen, innerArrayType)
        case _: MapType | _: StructType =>
          // Use add() instead of append loop - MapConverter/StructConverter.append not implemented
          buildComplexArrayElementVector(rows, column, totalLen, dataType.elementType)
        case _ =>
          val ev = new OmniColumnVector(totalLen, dataType.elementType, true)
          for (row <- rows) {
            val arrayData = if (row == null) null else row.getArray(column)
            if (arrayData != null) {
              val arrayLength = arrayData.numElements
              for (i <- 0 until arrayLength) {
                elementConverter.append(arrayData, i, ev)
              }
            }
          }
          ev
      }

      cv.setChild(elementVector, 0)
      cv.setOffsets(offsets.toArray)
      cv.updateVec()
      cv.putNulls(0, nulls.toArray, size)
      cv
    }
  }

  /**
   * Build element vector for Array<Map> or Array<Struct> by flattening elements and using add().
   * Avoids MapConverter/StructConverter.append() which is not implemented.
   */
  private def buildComplexArrayElementVector(
      rows: Seq[SpecializedGetters],
      column: Int,
      totalLen: Int,
      elementType: DataType): OmniColumnVector = {
    val elementRows = new ListBuffer[SpecializedGetters]()
    for (row <- rows) {
      val arrayData = if (row == null) null else row.getArray(column)
      if (arrayData != null) {
        for (i <- 0 until arrayData.numElements) {
          elementRows += new ArrayElementGetter(arrayData, i)
        }
      }
    }
    val nullable = elementType match {
      case m: MapType => m.valueContainsNull
      case _ => true
    }
    val converter = getConverterForType(elementType, nullable)
    converter.add(elementRows.toSeq, 0, elementRows.size).asInstanceOf[OmniColumnVector]
  }

  /** Wrapper to expose array element at index as SpecializedGetters for add(). */
  private class ArrayElementGetter(arrayData: ArrayData, index: Int)
      extends InternalRow with Serializable {
    override def numFields: Int = 1
    override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()
    override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()
    override def copy(): InternalRow = throw new UnsupportedOperationException()
    override def isNullAt(ordinal: Int): Boolean = arrayData.isNullAt(index)
    override def getBoolean(ordinal: Int): Boolean = arrayData.getBoolean(index)
    override def getByte(ordinal: Int): Byte = arrayData.getByte(index)
    override def getShort(ordinal: Int): Short = arrayData.getShort(index)
    override def getInt(ordinal: Int): Int = arrayData.getInt(index)
    override def getLong(ordinal: Int): Long = arrayData.getLong(index)
    override def getFloat(ordinal: Int): Float = arrayData.getFloat(index)
    override def getDouble(ordinal: Int): Double = arrayData.getDouble(index)
    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
      arrayData.getDecimal(index, precision, scale)
    override def getUTF8String(ordinal: Int): UTF8String = arrayData.getUTF8String(index)
    override def getBinary(ordinal: Int): Array[Byte] = arrayData.getBinary(index)
    override def getInterval(ordinal: Int): CalendarInterval =
      arrayData.getInterval(index)
    override def getStruct(ordinal: Int, numFields: Int): InternalRow =
      if (ordinal == 0 && !arrayData.isNullAt(index)) arrayData.getStruct(index, numFields) else null
    override def getArray(ordinal: Int): ArrayData =
      if (ordinal == 0 && !arrayData.isNullAt(index)) arrayData.getArray(index) else null
    override def getMap(ordinal: Int): MapData =
      if (ordinal == 0 && !arrayData.isNullAt(index)) arrayData.getMap(index) else null
    override def get(ordinal: Int, dataType: DataType): AnyRef =
      if (ordinal == 0 && !arrayData.isNullAt(index)) arrayData.get(index, dataType) else null
  }

  /** Build element vector for nested array (e.g. Array<Array<Long>>, Array<Array<Int>>, Array<Array<String>>). Only supports 2 levels. */
  private def buildNestedArrayElementVector(
      rows: Seq[SpecializedGetters],
      column: Int,
      innerArrayCount: Int,
      innerArrayType: ArrayType): OmniColumnVector = {
    val innerLengths = new ListBuffer[Int]
    for (row <- rows) {
      val arrayData = if (row == null) null else row.getArray(column)
      if (arrayData != null) {
        for (i <- 0 until arrayData.numElements) {
          val inner = arrayData.getArray(i)
          val len = if (inner == null) 0 else inner.numElements
          innerLengths += len
        }
      }
    }
    val actualInnerCount = innerLengths.size
    val innerOffsetsFinal = new Array[Int](actualInnerCount + 1)
    innerOffsetsFinal(0) = 0
    for (i <- 1 to actualInnerCount) {
      innerOffsetsFinal(i) = innerOffsetsFinal(i - 1) + innerLengths(i - 1)
    }
    val totalLeaf = innerOffsetsFinal(actualInnerCount)
    // Build leaf vector for Array<Array<Elem>> (supports Long, Int, Double, Float, Short, String, Timestamp, Boolean, Byte)
    innerArrayType.elementType match {
      case BooleanType =>
        val boolVector = new OmniColumnVector(totalLeaf, BooleanType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  boolVector.appendBoolean(inner.getBoolean(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, boolVector)
      case ByteType =>
        val byteVector = new OmniColumnVector(totalLeaf, ByteType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  byteVector.appendByte(inner.getByte(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, byteVector)
      case LongType | TimestampType =>
        val longVector = new OmniColumnVector(totalLeaf, LongType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  longVector.appendLong(inner.getLong(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, longVector)
      case IntegerType | DateType =>
        val intVector = new OmniColumnVector(totalLeaf, IntegerType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  intVector.appendInt(inner.getInt(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, intVector)
      case DoubleType =>
        val doubleVector = new OmniColumnVector(totalLeaf, DoubleType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  doubleVector.appendDouble(inner.getDouble(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, doubleVector)
      case FloatType =>
        val floatVector = new OmniColumnVector(totalLeaf, FloatType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  floatVector.appendFloat(inner.getFloat(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, floatVector)
      case ShortType =>
        val shortVector = new OmniColumnVector(totalLeaf, ShortType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  shortVector.appendShort(inner.getShort(j))
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, shortVector)
      case StringType =>
        val stringVector = new OmniColumnVector(totalLeaf, StringType, true)
        for (row <- rows) {
          val arrayData = if (row == null) null else row.getArray(column)
          if (arrayData != null) {
            for (i <- 0 until arrayData.numElements) {
              val inner = arrayData.getArray(i)
              if (inner != null) {
                for (j <- 0 until inner.numElements) {
                  if (inner.isNullAt(j)) {
                    stringVector.appendNull
                  } else {
                    val s = inner.getUTF8String(j)
                    val data = s.getBytes
                    stringVector.asInstanceOf[OmniColumnVector].appendString(data.length, data, 0)
                  }
                }
              }
            }
          }
        }
        buildInnerArrayVector(innerArrayType, actualInnerCount, innerOffsetsFinal, stringVector)
      case _ =>
        throw new UnsupportedOperationException(
          s"buildNestedArrayElementVector only supports Array<Array<Boolean/Byte/Short/Int/Long/Float/Double/String/Date/Timestamp>>, got ${innerArrayType.elementType}")
    }
  }

  private def buildInnerArrayVector(
      innerArrayType: ArrayType,
      actualInnerCount: Int,
      innerOffsetsFinal: Array[Int],
      leafVector: OmniColumnVector): OmniColumnVector = {
    val vec = new OmniColumnVector(actualInnerCount, innerArrayType, true)
    vec.setChild(leafVector, 0)
    vec.setOffsets(innerOffsetsFinal)
    vec.updateVec()
    vec.putNulls(0, new Array[Byte](actualInnerCount), actualInnerCount)
    vec
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
        cv.asInstanceOf[OmniColumnVector].appendString(bytes.length, bytes, 0)
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

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row == null || row.isNullAt(column)) {
        cv.appendNull
      } else {
        cv.appendFloat(row.getFloat(column))
      }
    }

    override def add(rows: Seq[SpecializedGetters],
                     column: Int, size: Int): WritableColumnVector = {
      val cv = new OmniColumnVector(size, FloatType, true)
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
