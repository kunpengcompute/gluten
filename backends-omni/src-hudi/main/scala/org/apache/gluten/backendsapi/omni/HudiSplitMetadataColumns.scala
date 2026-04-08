/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.PartitionedFile

import java.lang.reflect.Method
import java.util.{Map => JMap}

import scala.util.control.NonFatal

/**
 * Fills per-split string map entries for Hudi virtual columns from the concrete
 * [[PartitionedFile]] implementation used by Hudi (no Hudi source changes).
 *
 * Compiled only with the `hudi` Maven profile; invoked from [[OmniIteratorApiImpl]] via reflection
 * when that profile is enabled.
 */
object HudiSplitMetadataColumns {

  val HOODIE_COMMIT_TIME: String = "_hoodie_commit_time"
  val HOODIE_COMMIT_SEQNO: String = "_hoodie_commit_seqno"
  val HOODIE_RECORD_KEY: String = "_hoodie_record_key"
  val HOODIE_PARTITION_PATH: String = "_hoodie_partition_path"
  val HOODIE_FILE_NAME: String = "_hoodie_file_name"

  def augment(file: PartitionedFile, names: Seq[String], out: JMap[String, String]): Unit = {
    val need = names.toSet
    if (!need.exists(_.startsWith("_hoodie_"))) {
      return
    }
    try {
      hudiBaseFile(file).foreach { bf =>
        invokeNoArgString(bf, "getCommitTime").foreach(
          v => put(out, HOODIE_COMMIT_TIME, v, need))
        // `_hoodie_record_key` is row-level; only fill if the runtime exposes a constant (unusual).
        invokeNoArgString(bf, "getRecordKey")
          .foreach(v => put(out, HOODIE_RECORD_KEY, v, need))
        // Seqno: try dedicated getters when present on the Hudi file bean.
        invokeNoArgString(bf, "getCommitSeqno")
          .orElse(
            Seq("getSeqId", "getCommitSeqNo").view.flatMap(invokeNoArgString(bf, _)).headOption)
          .foreach(v => put(out, HOODIE_COMMIT_SEQNO, v, need))
      }
    } catch {
      case NonFatal(_) =>
    }
    val pathStr = file.filePath.toString
    val decoded = try new Path(pathStr).toUri.getPath
    catch {
      case NonFatal(_) => pathStr
    }
    val baseName = decoded.split("/").filter(_.nonEmpty).lastOption.getOrElse("")
    if (need.contains(HOODIE_FILE_NAME) && baseName.nonEmpty) {
      putIfAbsent(out, HOODIE_FILE_NAME, baseName, need)
    }
    if (need.contains(HOODIE_PARTITION_PATH)) {
      val parentParts = decoded.split("/").filter(_.nonEmpty)
      val fileSeg = parentParts.lastOption.getOrElse("")
      val parent =
        if (parentParts.lengthCompare(1) > 0) {
          parentParts.dropRight(1).mkString("/")
        } else {
          ""
        }
      // Heuristic: relative directory path containing the data file (table layout dependent).
      if (parent.nonEmpty && fileSeg.nonEmpty) {
        putIfAbsent(out, HOODIE_PARTITION_PATH, parent, need)
      }
    }
    // When HoodieBaseFile reflection fails (common with Spark 3.5 + LegacyHoodieParquet path),
    // commit instant is still often embedded in the base file name: *_<commitTime>_<fileId>_<logId>.parquet
    fallbackCommitFromHudiFileName(baseName, need, out)
  }

  /** Only set when missing or empty, so successful reflection / Spark shims keep precedence. */
  private def putIfAbsent(
      out: JMap[String, String],
      key: String,
      value: String,
      need: Set[String]): Unit = {
    if (!need.contains(key) || value == null || value.isEmpty) {
      return
    }
    val cur = out.get(key)
    if (cur == null || cur.trim.isEmpty) {
      out.put(key, value)
    }
  }

  private def fallbackCommitFromHudiFileName(
      basename: String,
      need: Set[String],
      out: JMap[String, String]): Unit = {
    if (basename.isEmpty || !need.exists(k => k == HOODIE_COMMIT_TIME || k == HOODIE_COMMIT_SEQNO)) {
      return
    }
    parseCommitFromHudiBaseFileName(basename).foreach {
      case (commitTime, seqNo) =>
        putIfAbsent(out, HOODIE_COMMIT_TIME, commitTime, need)
        putIfAbsent(out, HOODIE_COMMIT_SEQNO, seqNo, need)
    }
  }

  /**
   * Parses Hudi COW/MOR base file naming where commit instant is a 14–19 digit token, followed by
   * numeric file / log ids, e.g. `..._20260328110401658_0_0.parquet`.
   */
  private def parseCommitFromHudiBaseFileName(basename: String): Option[(String, String)] = {
    val stem = basename.stripSuffix(".parquet").stripSuffix(".PARQUET")
    if (stem.isEmpty) {
      return None
    }
    val tokens = stem.split("_").filter(_.nonEmpty)
    val idx = tokens.indexWhere(t => t.length >= 14 && t.length <= 19 && t.forall(_.isDigit))
    if (idx < 0) {
      return None
    }
    val commitTime = tokens(idx)
    val seqNo = tokens.drop(idx).mkString("_")
    Some((commitTime, seqNo))
  }

  private def put(
      out: JMap[String, String],
      key: String,
      value: String,
      need: Set[String]): Unit = {
    if (value == null || value.isEmpty) {
      return
    }
    if (need.contains(key)) {
      out.put(key, value)
    }
  }

  private def hudiBaseFile(file: PartitionedFile): Option[AnyRef] = {
    val name = file.getClass.getName.toLowerCase(java.util.Locale.ROOT)
    val accessors = Seq("baseFile", "getBaseFile", "getHoodieBaseFile", "hoodieBaseFile")
    if (name.contains("hudi")) {
      return accessors.view.flatMap(invokeNoArg(file, _)).headOption
    }
    // Spark 3.5 + Hudi often uses PartitionedFile wrappers whose class name does not contain
    // "hudi"; still try common accessors so commit time / seqno reach Omni split info (otherwise
    // native scan emits NULL _hoodie_* and downstream operators can mis-bind columns).
    accessors.view.flatMap(invokeNoArg(file, _)).headOption
  }

  private def invokeNoArg(target: AnyRef, method: String): Option[AnyRef] = {
    TryOption {
      val m: Method = target.getClass.getMethod(method)
      m.invoke(target)
    }.filter(_ != null)
  }

  private def invokeNoArgString(target: AnyRef, method: String): Option[String] = {
    TryOption {
      val m = target.getClass.getMethod(method)
      m.invoke(target)
    }.filter(_ != null).map(_.toString).filter(_.nonEmpty)
  }

  private object TryOption {
    def apply[T](body: => T): Option[T] =
      try Some(body)
      catch {
        case NonFatal(_) => None
      }
  }
}
