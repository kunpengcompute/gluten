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
package org.apache.spark.sql.execution

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, IdentityBroadcastMode, Partitioning}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import scala.collection.mutable.ArrayBuffer;

// Utility methods to convert Vanilla broadcast relations from/to omni broadcast relations.
// FIXME: Truncate output with batch size.
object BroadcastUtils {
  def omniToSparkUnsafe[F, T](
      context: SparkContext,
      mode: BroadcastMode,
      from: Broadcast[F],
      fn: Iterator[ColumnarBatch] => Iterator[InternalRow]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) =>
        // ColumnarBuildSideRelation to HashedRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[BuildSideRelation]]
        val fromRelation = fromBroadcast.value.asReadOnlyCopy()
        var rowCount: Long = 0
        val toRelation = TaskResources.runUnsafe {
          val rowIterator = fn(fromRelation.deserialized.flatMap {
            cb =>
              rowCount += cb.numRows()
              Iterator(cb)
          })
          mode.transform(rowIterator, Some(rowCount))
        }
        // Rebroadcast Spark relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case IdentityBroadcastMode =>
        // ColumnarBuildSideRelation to HashedRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[BuildSideRelation]]
        val fromRelation = fromBroadcast.value.asReadOnlyCopy()
        val toRelation = TaskResources.runUnsafe {
          val rowIterator = fn(fromRelation.deserialized)
          val rowArray = new ArrayBuffer[InternalRow]

          /**
           * [[org.apache.gluten.execution.omniColumnarToRowExec.toRowIterator()]] creates a single
           * UnsafeRow. The iterator uses this same unsafe row and keep on changing the pointer to
           * point to new value. If we directly call rowIterator.toArray() then all the elements in
           * array points to same UnsafeRow object resulting in wrong output. here we need to create
           * a array having individual UnsafeRow object.
           */
          while (rowIterator.hasNext) {
            val unsafeRow = rowIterator.next().asInstanceOf[UnsafeRow]
            rowArray.append(unsafeRow.copy())
          }
          rowArray.toArray
        }
        // Rebroadcast Spark relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
  }

  def getBroadcastMode(partitioning: Partitioning): BroadcastMode = {
    partitioning match {
      case BroadcastPartitioning(mode) =>
        mode
      case _ =>
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning.toString)
    }
  }
}
