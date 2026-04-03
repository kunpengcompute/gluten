/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.integration.hudi

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Optimizer rule: replace Hudi [[org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand]]
 * with [[OmniGlutenHudiInsertBridgeCommand]] so INSERT SQL goes through DSv2
 * (append / overwrite / overwritePartitions) and Omni columnar Hudi write. Hudi JAR is not patched.
 */
class OmniHudiInsertRewriteRule(session: SparkSession) extends Rule[LogicalPlan] with Logging {

  import OmniHudiInsertRewriteRule._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!session.conf.get(CONF_ENABLE, "true").toBoolean) {
      return plan
    }
    if (!new GlutenConfig(session.sessionState.conf).enableGluten) {
      return plan
    }
    plan transformUp {
      case cmd: LogicalPlan if isHudiInsertCommand(cmd) =>
        tryExtract(cmd, session) match {
          case Some(bridge) => bridge
          case None => cmd
        }
      case other => other
    }
  }
}

object OmniHudiInsertRewriteRule extends Logging {

  /** When false, [[InsertIntoHoodieTableCommand]] is left unchanged. */
  val CONF_ENABLE: String = "spark.gluten.sql.omni.hudi.insert.sql.rewrite.enabled"

  private val HUDI_INSERT_CLASS = "org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand"

  /** For [[OmniRuleApi]] reflection registration. */
  def build(session: SparkSession): Rule[LogicalPlan] = new OmniHudiInsertRewriteRule(session)

  private def isHudiInsertCommand(plan: LogicalPlan): Boolean =
    plan.getClass.getName == HUDI_INSERT_CLASS

  private def tryExtract(
      cmd: LogicalPlan,
      session: SparkSession): Option[OmniGlutenHudiInsertBridgeCommand] = {
    val tableIdentOpt = extractQualifiedTableName(cmd, session)
    val queryOpt = readField[LogicalPlan](cmd, Seq("query"))
      .orElse(findQueryByProduct(cmd))
      .orElse(cmd.children.headOption)
    if (tableIdentOpt.isEmpty || queryOpt.isEmpty) {
      logWarning(
        s"[Gluten][Hudi+Omni] INSERT rewrite skipped: cannot extract table/query from ${cmd.getClass.getName}")
      return None
    }
    val overwrite =
      readField[Boolean](cmd, Seq("overwrite", "isOverwrite")).orElse(findOverwriteByProduct(cmd))
        .getOrElse(false)
    val partitionSpec = extractPartitionSpec(cmd)
    Some(
      OmniGlutenHudiInsertBridgeCommand(
        tableIdentOpt.get,
        queryOpt.get,
        partitionSpec,
        overwrite = overwrite,
        originalCommand = cmd))
  }

  private def extractPartitionSpec(cmd: LogicalPlan): Map[String, Option[String]] = {
    readField[Map[String, Option[String]]](cmd, Seq("partitionSpec")) match {
      case Some(m) => m
      case None =>
        readField[Map[String, String]](cmd, Seq("staticPartitions")) match {
          case Some(m) => m.map { case (k, v) => k -> Some(v) }
          case None =>
            findPartitionSpecByProduct(cmd) match {
              case Some(m: Map[_, _]) =>
                m.map { case (k, v) =>
                  k.toString -> (v match {
                    case None => None
                    case Some(s: String) => Some(s)
                    case Some(other) => Some(String.valueOf(other))
                    case s: String => Some(s)
                    case null => None
                    case other => Some(String.valueOf(other))
                  })
                }.toMap
              case _ => Map.empty
            }
        }
    }
  }

  private def readField[T](cmd: AnyRef, names: Seq[String]): Option[T] = {
    var c: Class[_] = cmd.getClass
    while (c != null) {
      for (n <- names) {
        try {
          val f = c.getDeclaredField(n)
          f.setAccessible(true)
          return Some(f.get(cmd).asInstanceOf[T])
        } catch {
          case _: NoSuchFieldException =>
        }
      }
      c = c.getSuperclass
    }
    None
  }

  // Hudi command fields are version-dependent; Product fallback avoids hard binding.
  private def findCatalogTableByProduct(cmd: AnyRef): Option[CatalogTable] = {
    cmd match {
      case p: Product =>
        p.productIterator.collectFirst { case t: CatalogTable => t }
      case _ => None
    }
  }

  private def findQueryByProduct(cmd: AnyRef): Option[LogicalPlan] = {
    cmd match {
      case p: Product =>
        p.productIterator.collectFirst { case lp: LogicalPlan => lp }
      case _ => None
    }
  }

  private def findOverwriteByProduct(cmd: AnyRef): Option[Boolean] = {
    cmd match {
      case p: Product =>
        p.productIterator.collectFirst { case b: Boolean => b }
      case _ => None
    }
  }

  private def findPartitionSpecByProduct(cmd: AnyRef): Option[Map[_, _]] = {
    cmd match {
      case p: Product =>
        p.productIterator.collectFirst { case m: Map[_, _] => m }
      case _ => None
    }
  }

  private def extractQualifiedTableName(cmd: LogicalPlan, session: SparkSession): Option[String] = {
    // 1) CatalogTable in fields/product
    readField[CatalogTable](cmd, Seq("table", "catalogTable"))
      .orElse(findCatalogTableByProduct(cmd))
      .map(ct => OmniHudiQualifiedTableName.forCatalogTable(session, ct))
      // 2) direct identifier-like field
      .orElse(
        readField[AnyRef](cmd, Seq("tableIdentifier", "tableId", "tableName"))
          .map(_.toString)
          .map(normalizeToV2Identifier(_, session)))
      // 3) parse from command string: "Relation spark_catalog.default.tbl[..."
      .orElse {
        val relationPattern = ".*Relation\\s+([^\\[]+)\\[.*".r
        cmd.simpleString(2000) match {
          case relationPattern(name) => Some(normalizeToV2Identifier(name.trim, session))
          case _ =>
            cmd.toString match {
              case relationPattern(name) => Some(normalizeToV2Identifier(name.trim, session))
              case _ => None
            }
        }
      }
  }

  private def normalizeToV2Identifier(name: String, session: SparkSession): String = {
    val trimmed = name.trim
    if (trimmed.split("\\.").length >= 3) {
      trimmed
    } else {
      val defaultCatalog = resolvePreferredCatalog(session)
      s"$defaultCatalog.$trimmed"
    }
  }

  private def resolvePreferredCatalog(session: SparkSession): String = {
    val sparkCatalogImpl = session.conf.getOption("spark.sql.catalog.spark_catalog").getOrElse("")
    if (sparkCatalogImpl.contains("HoodieCatalog")) {
      "spark_catalog"
    } else if (session.conf.getAll.keySet.exists(_.startsWith("spark.sql.catalog.hudi"))) {
      "hudi"
    } else {
      "spark_catalog"
    }
  }
}
