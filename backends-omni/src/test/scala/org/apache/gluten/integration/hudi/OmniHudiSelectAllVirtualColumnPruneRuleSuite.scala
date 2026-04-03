/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the License); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.integration.hudi

import java.nio.file.Files

import org.apache.hadoop.fs.Path

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[OmniHudiSelectAllVirtualColumnPruneRule]] on synthetic logical plans (no Hudi
 * catalog or native backend required).
 */
class OmniHudiSelectAllVirtualColumnPruneRuleSuite extends AnyFunSuite with BeforeAndAfterAll {

  /** Class name must contain `Hoodie` for [[org.apache.gluten.execution.HudiDatasourceDetection]]. */
  private final class FakeHoodieParquetFileFormatForTest extends ParquetFileFormat

  private final class PlainParquetFileFormatForTest extends ParquetFileFormat

  private var spark: SparkSession = _

  private def hdfsLogicalLeaf(dataSchema: StructType, format: ParquetFileFormat): LogicalRelation = {
    val base = Files.createTempDirectory("omni-hudi-prune-").toFile
    base.deleteOnExit()
    val dataPath = new java.io.File(base, "tbl")
    dataPath.mkdirs()
    val fileIndex =
      new InMemoryFileIndex(spark, Seq(new Path(dataPath.getAbsolutePath)), Map.empty, None)
    val rel = HadoopFsRelation(
      fileIndex,
      new StructType(),
      dataSchema,
      None,
      format,
      Map.empty)(spark)
    LogicalRelation(rel)
  }

  private def hudiScanLeaf(dataSchema: StructType): LogicalRelation =
    hdfsLogicalLeaf(dataSchema, new FakeHoodieParquetFileFormatForTest())

  private def nonHudiScanLeaf(dataSchema: StructType): LogicalRelation =
    hdfsLogicalLeaf(dataSchema, new PlainParquetFileFormatForTest())

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private def idAttr = AttributeReference("id", IntegerType, nullable = true)(NamedExpression.newExprId)
  private def hoodieAttr =
    AttributeReference("_hoodie_commit_time", StringType, nullable = true)(NamedExpression.newExprId)
  private def nameAttr =
    AttributeReference("name", StringType, nullable = true)(NamedExpression.newExprId)

  private def ruleEnabled: OmniHudiSelectAllVirtualColumnPruneRule = {
    spark.conf.set(GlutenConfig.OMNI_HUDI_SELECT_ALL_STRIP_VIRTUAL_COLUMNS.key, "true")
    OmniHudiSelectAllVirtualColumnPruneRule(new GlutenConfig(spark.sessionState.conf))
  }

  private def ruleDisabled: OmniHudiSelectAllVirtualColumnPruneRule = {
    spark.conf.set(GlutenConfig.OMNI_HUDI_SELECT_ALL_STRIP_VIRTUAL_COLUMNS.key, "false")
    OmniHudiSelectAllVirtualColumnPruneRule(new GlutenConfig(spark.sessionState.conf))
  }

  test("strip _hoodie_* from full positional Project when stripVirtualColumns=true") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("_hoodie_commit_time", StringType),
        StructField("name", StringType)))
    val leaf = hudiScanLeaf(schema)
    val o = leaf.output
    val plan = Project(o, leaf)
    val out = ruleEnabled.apply(plan).asInstanceOf[Project]
    assert(out.projectList.length === 2)
    assert(out.projectList.map(_.name) === Seq("id", "name"))
  }

  test("strip _hoodie_* when first column is wrapped in Cast (type coercion)") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("_hoodie_commit_time", StringType),
        StructField("name", StringType)))
    val leaf = hudiScanLeaf(schema)
    val Seq(idRef, hoodieRef, nameRef) = leaf.output
    val idCast =
      Alias(Cast(idRef, LongType), "id")(NamedExpression.newExprId, idRef.qualifier)
    val plan = Project(Seq(idCast, hoodieRef, nameRef), leaf)
    val out = ruleEnabled.apply(plan).asInstanceOf[Project]
    assert(out.projectList.length === 2)
    assert(out.projectList.head.isInstanceOf[Alias])
    assert(out.projectList.head.asInstanceOf[Alias].child.isInstanceOf[Cast])
    assert(out.projectList(1) === nameRef)
  }

  test("no strip for _hoodie_* column names when scan is not Hudi file format") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("_hoodie_commit_time", StringType),
        StructField("name", StringType)))
    val leaf = nonHudiScanLeaf(schema)
    val o = leaf.output
    val plan = Project(o, leaf)
    val out = ruleEnabled.apply(plan)
    assert(out === plan)
  }

  test("no change when stripVirtualColumns=false") {
    val id = idAttr
    val hoodie = hoodieAttr
    val name = nameAttr
    val leaf = LocalRelation(id, hoodie, name)
    val plan = Project(Seq(id, hoodie, name), leaf)
    val out = ruleDisabled.apply(plan)
    assert(out === plan)
  }

  test("no change when child has no _hoodie_ columns") {
    val id = idAttr
    val name = nameAttr
    val leaf = LocalRelation(id, name)
    val plan = Project(Seq(id, name), leaf)
    val out = ruleEnabled.apply(plan)
    assert(out === plan)
  }

  test("no change when Project does not cover full child output (not SELECT *)") {
    val id = idAttr
    val hoodie = hoodieAttr
    val name = nameAttr
    val leaf = LocalRelation(id, hoodie, name)
    val plan = Project(Seq(id, name), leaf)
    val out = ruleEnabled.apply(plan)
    assert(out === plan)
  }
}
