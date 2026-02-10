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

import org.apache.gluten.execution.RowToColumnarExecBase
import org.apache.gluten.execution.{ProjectExecTransformer, SortExecTransformer, WholeStageTransformer}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, SparkPlan}
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages.transformStageCounter
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.StructType

import java.{util => ju}

class OmniParquetFormatWriterInjects extends GlutenFormatWriterInjectsBase {

  override def nativeConf(
                           options: Map[String, String],
                           compressionCodec: String): java.util.Map[String, String] = ju.Collections.emptyMap()

  override def formatName: String = "parquet"

  override def createOutputWriter(
                                   outputPath: String,
                                   dataSchema: StructType,
                                   context: TaskAttemptContext,
                                   nativeConf: ju.Map[String, String]): OutputWriter = {
    new OmniParquetOutputWriter(outputPath, dataSchema, context)
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, options, files)
  }

  def execWriterWrappedSparkPlan(plan: SparkPlan): SparkPlan = {
    if (plan.isInstanceOf[FakeRowAdaptor]) {
      // here, the FakeRowAdaptor is simply a R2C converter
      return plan
    }

    // FIXME: HeuristicTransform is costly. Re-applying it may cause performance issues.
    val transformed = plan match {
      case rowToColumnarExecBase: RowToColumnarExecBase => plan
      case _ => transform(plan)
    }

    def injectAdapter(p: SparkPlan): SparkPlan = p match {
      case p: ProjectExecTransformer => p.mapChildren(injectAdapter)
      case s: SortExecTransformer => s.mapChildren(injectAdapter)
      case _ => ColumnarCollapseTransformStages.wrapInputIteratorTransformer(p)
    }

    val transformedWithAdapter = injectAdapter(transformed)
    val wst = WholeStageTransformer(transformedWithAdapter, materializeInput = true)(
      transformStageCounter.incrementAndGet())
    FakeRowAdaptor(wst)
  }
}
