/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import scala.util.Try

/**
 * Hudi offload rules for Omni: scan via pre-transform (before [[org.apache.gluten.extension.columnar.offload.OffloadOthers]]),
 * write via reflection from `src-hudi`.
 *
 * Hudi offload is enabled only when native Hudi Spark SQL integration is configured:
 * `spark.sql.extensions` includes [[HoodieSparkSessionExtension]] and
 * `spark.sql.catalog.spark_catalog` is [[HoodieCatalog]], then the `hudi` DataSource provider
 * must also be resolvable on classpath.
 */
object HudiOffloadRegistry {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  private val HoodieSparkSessionExtension =
    "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
  private val HoodieCatalog = "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
  private val SparkCatalogConfKey = "spark.sql.catalog.spark_catalog"

  private lazy val hudiRuntimeAvailable: Boolean = isHudiSessionConfigured()

  /** Write rules only; Hudi scan uses [[injectPreTransformRules]]. */
  def offloads: Seq[OffloadSingleNode] = {
    if (hudiRuntimeAvailable) {
      loadWriteOffloads()
    } else {
      Seq.empty
    }
  }

  def injectPreTransformRules(injector: LegacyInjector): Unit = {
    if (!hudiRuntimeAvailable) {
      return
    }
    loadScanPreRule().foreach { rule =>
      injector.injectPreTransform(_ => rule)
    }
  }

  private def readSparkConf(key: String, default: String = ""): String = {
    Try(SparkContext.getOrCreate().getConf.getOption(key)).toOption.flatten.getOrElse(default)
  }

  private def isHudiSessionConfigured(): Boolean = {
    val extensionsKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
    val sparkConfExtensions = readSparkConf(extensionsKey)
    val sqlConfExtensions =
      Try(SQLConf.get.getConfString(extensionsKey, "")).getOrElse("")
    // Prefer SparkConf: Gluten injectRules runs during session extension bootstrap, before
    // SQLConf thread-local is fully populated. Catalog configs also live on SparkConf only.
    val extensions = if (sparkConfExtensions.nonEmpty) sparkConfExtensions else sqlConfExtensions
    val extensionList = extensions.split(",").map(_.trim).filter(_.nonEmpty)
    val extensionsConfigured = extensionList.contains(HoodieSparkSessionExtension)

    val sparkConfCatalog = readSparkConf(SparkCatalogConfKey)
    val sqlConfCatalog = Try(SQLConf.get.getConfString(SparkCatalogConfKey, "")).getOrElse("")
    val catalog = if (sparkConfCatalog.nonEmpty) sparkConfCatalog else sqlConfCatalog
    val catalogConfigured = catalog == HoodieCatalog

    extensionsConfigured && catalogConfigured
  }

  private def loadScanPreRule(): Option[Rule[SparkPlan]] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadOmniHudiScanPreRule$")
      Class.forName("org.apache.gluten.execution.HudiScanTransformer")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("apply")
      Some(method.invoke(module).asInstanceOf[Rule[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  private def loadWriteOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadHudiWrite")
      val method = clazz.getMethod("offloads")
      method.invoke(null).asInstanceOf[Seq[OffloadSingleNode]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }
}
