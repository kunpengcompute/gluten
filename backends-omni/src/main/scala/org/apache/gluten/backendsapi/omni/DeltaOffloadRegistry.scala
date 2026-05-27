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
 * Delta offload registry for Omni.
 *
 * This class stays in the default Omni source set and loads Delta-specific classes reflectively,
 * so backends-omni can still be built without `-Pdelta`.
 */
object DeltaOffloadRegistry {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  private val DeltaSparkSessionExtension = "io.delta.sql.DeltaSparkSessionExtension"
  private val DeltaCatalog = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  private val SparkCatalogConfKey = "spark.sql.catalog.spark_catalog"

  private lazy val deltaRuntimeAvailable: Boolean = isDeltaSessionConfigured()

  def offloads: Seq[OffloadSingleNode] = {
    if (deltaRuntimeAvailable) {
      loadScanOffloads() ++ loadWriteOffloads()
    } else {
      Seq.empty
    }
  }

  def injectPreTransformRules(injector: LegacyInjector): Unit = {
    if (!deltaRuntimeAvailable) {
      return
    }
    loadScanPreRule().foreach { rule =>
      log.warn("[Gluten][OmniDelta][registry] Registering Delta scan pre-transform rule")
      injector.injectPreTransform(_ => rule)
    }
  }

  def injectPostTransformRules(injector: LegacyInjector): Unit = {
    if (!deltaRuntimeAvailable) {
      return
    }
    loadDeltaPostTransformRules().foreach { rule =>
      injector.injectPostTransform(_ => rule)
    }
  }

  def pushDownFilterToScan(plan: SparkPlan): Option[SparkPlan] = {
    if (!deltaRuntimeAvailable) {
      return None
    }
    loadDeltaFilterPushDown()
      .flatMap(_(plan))
  }

  def wrapTransformRule(rule: Rule[SparkPlan]): Rule[SparkPlan] = {
    if (!deltaRuntimeAvailable) {
      return rule
    }
    loadDeltaMetadataTransformInterceptor()
      .map(_(rule))
      .getOrElse(rule)
  }

  private def readSparkConf(key: String, default: String = ""): String = {
    Try(SparkContext.getOrCreate().getConf.getOption(key)).toOption.flatten.getOrElse(default)
  }

  private def isDeltaSessionConfigured(): Boolean = {
    val extensionsKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
    val sparkConfExtensions = readSparkConf(extensionsKey)
    val sqlConfExtensions = Try(SQLConf.get.getConfString(extensionsKey, "")).getOrElse("")
    val extensions = if (sparkConfExtensions.nonEmpty) sparkConfExtensions else sqlConfExtensions
    val extensionsConfigured =
      extensions.split(",").map(_.trim).filter(_.nonEmpty).contains(DeltaSparkSessionExtension)

    val sparkConfCatalog = readSparkConf(SparkCatalogConfKey)
    val sqlConfCatalog = Try(SQLConf.get.getConfString(SparkCatalogConfKey, "")).getOrElse("")
    val catalog = if (sparkConfCatalog.nonEmpty) sparkConfCatalog else sqlConfCatalog
    val catalogConfigured = catalog == DeltaCatalog

    extensionsConfigured && catalogConfigured
  }

  private def loadScanPreRule(): Option[Rule[SparkPlan]] = {
    try {
      val clazz =
        Class.forName("org.apache.gluten.extension.columnar.offload.OffloadOmniDeltaScanPreRule$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("apply")
      Some(method.invoke(module).asInstanceOf[Rule[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  private def loadScanOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadOmniDeltaScan")
      val ctor = clazz.getConstructor()
      Seq(ctor.newInstance().asInstanceOf[OffloadSingleNode])
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadWriteOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadDeltaWrite")
      val method = clazz.getMethod("offloads")
      method.invoke(null).asInstanceOf[Seq[OffloadSingleNode]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadDeltaPostTransformRules(): Seq[Rule[SparkPlan]] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.DeltaPostTransformRules$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("rules")
      method.invoke(module).asInstanceOf[Seq[Rule[SparkPlan]]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadDeltaFilterPushDown(): Option[SparkPlan => Option[SparkPlan]] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.PushDownFilterToOmniDeltaScan$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("tryPushDown", classOf[SparkPlan])
      Some((plan: SparkPlan) => method.invoke(module, plan).asInstanceOf[Option[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  private def loadDeltaMetadataTransformInterceptor(): Option[Rule[SparkPlan] => Rule[SparkPlan]] = {
    try {
      val clazz =
        Class.forName("org.apache.gluten.extension.columnar.offload.DeltaMetadataTransformInterceptor$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("apply", classOf[Rule[SparkPlan]])
      Some((rule: Rule[SparkPlan]) => method.invoke(module, rule).asInstanceOf[Rule[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }
}
