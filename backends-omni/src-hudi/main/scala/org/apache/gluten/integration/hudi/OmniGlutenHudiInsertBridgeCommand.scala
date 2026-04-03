/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.integration.hudi

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

/**
 * Runs Hudi INSERT via [[org.apache.spark.sql.DataFrameWriterV2]] (append / overwrite /
 * overwritePartitions) so the physical plan uses DSv2 write Exec nodes and Gluten can offload to
 * [[org.apache.gluten.execution.AbstractHudiWriteExec]] variants. Does not fall back to the native
 * [[InsertIntoHoodieTableCommand]] on analysis or link errors.
 */
case class OmniGlutenHudiInsertBridgeCommand(
    tableIdent: String,
    query: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    overwrite: Boolean,
    originalCommand: LogicalPlan)
  extends RunnableCommand
  with Logging {

  override def children: Seq[LogicalPlan] = Seq(query)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    copy(query = newChildren.head)
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val op =
      if (overwrite) {
        if (partitionSpec.exists(_._2.isEmpty)) "overwritePartitions()"
        else if (partitionSpec.isEmpty) "overwrite(lit(true))"
        else "overwrite(partition predicate)"
      } else {
        "append()"
      }
    logWarning(
      s"[Gluten][Hudi+Omni] INSERT SQL rewritten: writeTo($tableIdent).$op for Omni DSv2 columnar write.")

    val dsObj = invokeOfRows(spark, query)

    var ds: AnyRef = dsObj
    for ((partName, Some(litVal)) <- partitionSpec) {
      val litCol = lit(litVal)
      val m = ds.getClass.getMethod("withColumn", classOf[String], classOf[org.apache.spark.sql.Column])
      ds = m.invoke(ds, partName, litCol).asInstanceOf[AnyRef]
    }

    try {
      executeWrite(ds, tableIdent)
    } catch {
      case ite: java.lang.reflect.InvocationTargetException =>
        val cause = Option(ite.getCause).getOrElse(ite)
        if (isV1WriteError(cause)) {
          handleV1WriteByDirectPath(spark, ds, cause)
        } else {
          handleWriteFailure(cause)
        }
      case t: Throwable =>
        if (isV1WriteError(t)) {
          handleV1WriteByDirectPath(spark, ds, t)
        } else {
          handleWriteFailure(t)
        }
    }
    Seq.empty
  }

  private def executeWrite(ds: AnyRef, identifier: String): Unit = {
    val writerV2 = ds.getClass.getMethod("writeTo", classOf[String]).invoke(ds, identifier)
    if (overwrite) {
      if (partitionSpec.exists(_._2.isEmpty)) {
        writerV2.getClass.getMethod("overwritePartitions").invoke(writerV2)
      } else if (partitionSpec.isEmpty) {
        writerV2.getClass
          .getMethod("overwrite", classOf[org.apache.spark.sql.Column])
          .invoke(writerV2, lit(true))
      } else {
        val parts = partitionSpec.collect { case (k, Some(v)) => col(k) === lit(v) }
        val cond =
          if (parts.isEmpty) lit(true)
          else parts.reduce(_ && _)
        writerV2.getClass
          .getMethod("overwrite", classOf[org.apache.spark.sql.Column])
          .invoke(writerV2, cond)
      }
    } else {
      writerV2.getClass.getMethod("append").invoke(writerV2)
    }
  }

  private def alternativeCatalogIdentifiers(spark: SparkSession, current: String): Seq[String] = {
    val parts = current.split("\\.", 3)
    if (parts.length != 3) {
      return Seq.empty
    }
    val db = parts(1)
    val table = parts(2)
    val confEntries = spark.conf.getAll
    confEntries.collect {
      case (k, v)
          if k.startsWith("spark.sql.catalog.") &&
            !k.substring("spark.sql.catalog.".length).contains(".") &&
            v.contains("HoodieCatalog") =>
        s"${k.substring("spark.sql.catalog.".length)}.$db.$table"
    }.toSeq.distinct.filterNot(_ == current)
  }

  private def handleV1WriteByDirectPath(
      spark: SparkSession,
      ds: AnyRef,
      cause: Throwable): Unit = {
    val ct = extractCatalogTable(originalCommand).orElse(loadCatalogTableByIdent(spark, tableIdent)).getOrElse {
      throw new UnsupportedOperationException(
        s"[Gluten][Hudi+Omni] V1 table detected for $tableIdent and no CatalogTable extracted; " +
          "cannot apply direct Hudi path write.",
        cause)
    }
    val rawDs = ds.asInstanceOf[org.apache.spark.sql.Dataset[Row]]
    val targetCols = ct.schema.fieldNames.toSeq.filterNot(_.startsWith("_hoodie_"))
    val namedDs =
      if (rawDs.columns.length == targetCols.length) {
        rawDs.toDF(targetCols: _*)
      } else {
        rawDs
      }
    // Literal rows (e.g. INSERT VALUES 98.5) often resolve as DecimalType, while the catalog
    // declares DOUBLE; Hudi then rejects writer vs table Avro unions. Align to catalog types.
    val alignedDs = alignDataFrameToCatalogSchema(namedDs, ct.schema, targetCols)
    val path = ct.location.toString
    val props = ct.properties
    val tableName = ct.identifier.table
    val tableType = props.getOrElse("type", "cow")
    val recordKey = props.getOrElse("primaryKey", "")
    val preCombine = props.getOrElse("preCombineField", "")
    val partitionCols = ct.partitionColumnNames
    // CREATE TABLE sets a KeyGenerator in Hudi table metadata; multi-partition tables use
    // ComplexKeyGenerator. Forcing SimpleKeyGenerator causes HoodieWriterUtils.validateTableConfig
    // to fail. Prefer TBLPROPERTIES, else match Hudi defaults by partition column count.
    val keygenClass = resolveKeyGeneratorClass(props, partitionCols)
    // TBLPROPERTIES often carry hoodie.* (e.g. hoodie.metadata.enable). Apply them before the
    // fixed write options so explicit options below still win for record key / precombine / etc.
    var writer = alignedDs.write.format("hudi")
    props.foreach {
      case (k, v) if k.startsWith("hoodie.") =>
        writer = writer.option(k, v)
      case _ =>
    }
    val hudiWriteOperation = resolveHudiDatasourceWriteOperation()
    writer = writer
      .option("hoodie.table.name", tableName)
      .option(
        "hoodie.datasource.write.table.type",
        if (tableType.equalsIgnoreCase("mor")) "MERGE_ON_READ" else "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", hudiWriteOperation)
      .option("hoodie.datasource.write.keygenerator.class", keygenClass)
    if (recordKey.nonEmpty) {
      writer = writer.option("hoodie.datasource.write.recordkey.field", recordKey)
    }
    if (preCombine.nonEmpty) {
      writer = writer.option("hoodie.datasource.write.precombine.field", preCombine)
    }
    if (partitionCols.nonEmpty) {
      writer = writer
        .option("hoodie.datasource.write.partitionpath.field", partitionCols.mkString(","))
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
    }
    logWarning(
      s"[Gluten][Hudi+Omni] writeTo($tableIdent) resolved to V1 table; use direct Hudi path write at $path.")
    // insert_overwrite* semantics come from hoodie.datasource.write.operation. Use Append so Spark
    // does not delete the table base path (including .metadata / timeline) before Hudi runs.
    // V1 path write triggers Hudi `toRdd`; literal/OneRowRelation plans can hit Gluten columnar
    // mismatch (ProjectExecTransformer vs RowToOmniColumnar). Run this save with Gluten off.
    withGlutenDisabled(spark) {
      writer.mode(SaveMode.Append).save(path)
    }
    refreshTableAfterHudiWrite(spark, tableIdent)
  }

  /** Temporarily disable Gluten so the active session runs the enclosed action with vanilla Spark. */
  private def withGlutenDisabled[T](spark: SparkSession)(f: => T): T = {
    val key = GlutenConfig.GLUTEN_ENABLED.key
    val previous = spark.conf.getOption(key)
    try {
      spark.conf.set(key, "false")
      f
    } finally {
      previous match {
        case Some(v) => spark.conf.set(key, v)
        case None => spark.conf.unset(key)
      }
    }
  }

  /**
   * Mirror [[executeWrite]] overwrite modes for the V1 Hudi writer (see Hudi
   * `hoodie.datasource.write.operation`).
   */
  private def resolveHudiDatasourceWriteOperation(): String = {
    if (!overwrite) {
      "insert"
    } else if (partitionSpec.exists(_._2.isEmpty)) {
      // DataFrameWriterV2.overwritePartitions — replace partitions present in the batch
      "insert_overwrite"
    } else if (partitionSpec.isEmpty) {
      // overwrite(lit(true)) — full table (incl. non-partitioned)
      "insert_overwrite_table"
    } else {
      // overwrite(partition predicate) — static partitions, scope matches inserted rows
      "insert_overwrite"
    }
  }

  /** Same keys Hudi / Spark SQL typically store on the catalog table. */
  private def resolveKeyGeneratorClass(
      props: Map[String, String],
      partitionCols: Seq[String]): String = {
    val fromCatalog = props
      .get("hoodie.datasource.write.keygenerator.class")
      .orElse(props.get("hoodie.keygenerator.class"))
      .map(_.trim)
      .filter(_.nonEmpty)
    fromCatalog.getOrElse {
      if (partitionCols.isEmpty) {
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
      } else if (partitionCols.length == 1) {
        "org.apache.hudi.keygen.SimpleKeyGenerator"
      } else {
        "org.apache.hudi.keygen.ComplexKeyGenerator"
      }
    }
  }

  /**
   * Cast each non-Hudi catalog column to its [[CatalogTable]] type so INSERT literals match the
   * table DDL (e.g. DECIMAL-detected literals vs DOUBLE column).
   */
  private def alignDataFrameToCatalogSchema(
      df: org.apache.spark.sql.DataFrame,
      catalogSchema: StructType,
      columnNames: Seq[String]): org.apache.spark.sql.DataFrame = {
    val castExprs = columnNames.flatMap { n =>
      if (!df.columns.contains(n)) {
        None
      } else {
        Some(col(n).cast(catalogSchema(n).dataType).as(n))
      }
    }
    val rest = df.columns.filter(!columnNames.contains(_)).map(col)
    if (rest.isEmpty) {
      df.select(castExprs: _*)
    } else {
      df.select(castExprs ++ rest: _*)
    }
  }

  /**
   * Path-based Hudi writes do not update Spark's cached [[org.apache.hudi.EmptyRelation]] entry
   * for V1 tables; without refresh, SELECT may scan EmptyRelation (empty rows / Gluten column
   * mismatch on Project + RowToOmniColumnar).
   */
  private def refreshTableAfterHudiWrite(spark: SparkSession, threePartIdent: String): Unit = {
    try {
      spark.sql(s"REFRESH TABLE $threePartIdent")
      logWarning(
        s"[Gluten][Hudi+Omni] Refreshed catalog metadata for $threePartIdent after V1 Hudi path write.")
    } catch {
      case e: Throwable =>
        logWarning(
          s"[Gluten][Hudi+Omni] REFRESH TABLE $threePartIdent failed (${e.getMessage}). " +
            "Run REFRESH TABLE manually before SELECT if results are empty or you see EmptyRelation in the plan.")
    }
  }

  private def extractCatalogTable(cmd: AnyRef): Option[CatalogTable] = {
    var c: Class[_] = cmd.getClass
    while (c != null) {
      try {
        val f = c.getDeclaredField("table")
        f.setAccessible(true)
        f.get(cmd) match {
          case t: CatalogTable => return Some(t)
          case _ =>
        }
      } catch {
        case _: NoSuchFieldException =>
      }
      try {
        val f = c.getDeclaredField("catalogTable")
        f.setAccessible(true)
        f.get(cmd) match {
          case t: CatalogTable => return Some(t)
          case _ =>
        }
      } catch {
        case _: NoSuchFieldException =>
      }
      c = c.getSuperclass
    }
    cmd match {
      case p: Product =>
        p.productIterator.collectFirst { case t: CatalogTable => t }
      case _ => None
    }
  }

  private def loadCatalogTableByIdent(spark: SparkSession, ident: String): Option[CatalogTable] = {
    val parts = ident.split("\\.")
    if (parts.length < 3) {
      return None
    }
    val db = parts(parts.length - 2)
    val table = parts(parts.length - 1)
    try {
      Some(spark.sessionState.catalog.getTableMetadata(TableIdentifier(table, Some(db))))
    } catch {
      case _: Throwable => None
    }
  }

  private def handleWriteFailure(t: Throwable): Nothing = {
    t match {
      case ae: AnalysisException if isV1WriteError(ae) =>
        throw new UnsupportedOperationException(
          s"[Gluten][Hudi+Omni] DSv2 writeTo path requires a V2 Hudi table/catalog, " +
            s"but got V1 table: $tableIdent. Configure Hudi to use a V2 catalog (HoodieCatalog) " +
            s"for this table, otherwise this statement cannot be offloaded to Omni.",
          ae)
      case other =>
        throw other
    }
  }

  private def isV1WriteError(t: Throwable): Boolean = {
    val msg = Option(t.getMessage).getOrElse("")
    msg.contains("Cannot write into v1 table")
  }

  private def invokeOfRows(spark: SparkSession, plan: LogicalPlan): AnyRef = {
    val moduleClass = Class.forName("org.apache.spark.sql.Dataset$")
    val module = moduleClass.getField("MODULE$").get(null)
    val m = moduleClass.getMethod(
      "ofRows",
      classOf[SparkSession],
      classOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan])
    try {
      m.invoke(module, spark, plan).asInstanceOf[AnyRef]
    } catch {
      case ite: java.lang.reflect.InvocationTargetException =>
        throw Option(ite.getCause).getOrElse(ite)
    }
  }
}