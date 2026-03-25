/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.backendsapi.omni

import org.apache.spark.sql.connector.write.Write

/**
 * Hudi write utilities (aligned with IcebergWriteUtil): detect Hudi Write and obtain
 * directory (base path), queryId, codec for columnar write. Uses reflection for Hudi class layout.
 *
 * @since 2026
 */

object HudiWriteUtil {

  /** True if this Write is from Hudi (Hoodie) so we can offload AppendDataExec to columnar path. */
  def supportsWrite(write: Write): Boolean = {
    val name = write.getClass.getName
    name.contains("hudi") || name.contains("Hoodie")
  }

  /** Base path (directory) for writing data. Obtained via reflection from Hudi Write. */
  def getDirectory(write: Write): String = {
    val w = write.getClass
    try {
      // Try common Hudi Write field names for base path / table
      val basePathField = w.getDeclaredField("basePath")
      basePathField.setAccessible(true)
      basePathField.get(write).asInstanceOf[String]
    } catch {
      case _: NoSuchFieldException =>
        try {
          val batchWriteField = w.getDeclaredField("batchWrite")
          batchWriteField.setAccessible(true)
          val batchWrite = batchWriteField.get(write)
          val tableField = batchWrite.getClass.getDeclaredField("table")
          tableField.setAccessible(true)
          val table = tableField.get(batchWrite)
          val locField = table.getClass.getMethod("getBasePath")
          locField.invoke(table).asInstanceOf[String]
        } catch {
          case _: Throwable =>
            throw new UnsupportedOperationException(
              "Cannot get base path from Hudi Write: " + w.getName)
        }
    }
  }

  /** Query id for this write (e.g. Spark query id). */
  def getQueryId(write: Write): String = {
    val w = write.getClass
    try {
      val f = w.getDeclaredField("queryId")
      f.setAccessible(true)
      f.get(write).asInstanceOf[String]
    } catch {
      case _: NoSuchFieldException =>
        try {
          val f = w.getDeclaredField("instantTime")
          f.setAccessible(true)
          f.get(write).asInstanceOf[String]
        } catch {
          case _: NoSuchFieldException => java.util.UUID.randomUUID().toString
        }
    }
  }

  /** Parquet compression codec (e.g. "snappy", "gzip", "none"). */
  def getCodec(write: Write): String = {
    val w = write.getClass
    try {
      val opts = getOptionsField(write, w)
      if (opts != null) {
        val codec = opts.getOrDefault("hoodie.parquet.compression.codec", "snappy")
        if (codec.equalsIgnoreCase("uncompressed")) "none" else codec
      } else "snappy"
    } catch {
      case _: Throwable => "snappy"
    }
  }

  private def getOptionsField(write: Write, w: Class[_]): java.util.Map[String, String] = {
    try {
      val f = w.getDeclaredField("options")
      f.setAccessible(true)
      f.get(write).asInstanceOf[java.util.Map[String, String]]
    } catch {
      case _: NoSuchFieldException =>
        try {
          val f = w.getDeclaredField("writeParams")
          f.setAccessible(true)
          val params = f.get(write)
          if (params != null) {
            val m = params.getClass.getDeclaredField("options")
            m.setAccessible(true)
            m.get(params).asInstanceOf[java.util.Map[String, String]]
          } else null
        } catch {
          case _: Throwable => null
        }
    }
  }
}
