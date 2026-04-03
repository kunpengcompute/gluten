/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.integration.hudi

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.SparkSession

/** Builds `writeTo` identifier for a [[CatalogTable]] (HoodieCatalog / spark_catalog). */
private[hudi] object OmniHudiQualifiedTableName {

  def forCatalogTable(spark: SparkSession, table: CatalogTable): String = {
    val tid = table.identifier
    val catalogName =
      try {
        spark.sessionState.catalogManager.currentCatalog.name()
      } catch {
        case _: Throwable => "spark_catalog"
      }
    val database = tid.database.getOrElse {
      try {
        val db = spark.catalog.currentDatabase
        if (db != null && db.nonEmpty) db else "default"
      } catch {
        case _: Throwable => "default"
      }
    }
    s"$catalogName.$database.${tid.table}"
  }
}
