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
package org.apache.spark.sql.delta.files

import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * Wrapper around [[DelayedCommitProtocol]] so omni native writes can access the files staged by
 * Delta's delayed commit flow while still returning the standard Delta [[AddFile]] actions.
 */
class FileDelayedCommitProtocol(
    jobId: String,
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String])
  extends DelayedCommitProtocol(jobId, outputPath, randomPrefixLength, subdir) {

  override def getFileName(
      taskContext: TaskAttemptContext,
      ext: String,
      partitionValues: Map[String, String]): String = {
    super.getFileName(taskContext, ext, partitionValues)
  }

  def updateAddedFiles(files: Seq[(Map[String, String], String)]): Unit = {
    assert(addedFiles.isEmpty)
    addedFiles ++= files
  }

  override def parsePartitions(dir: String): Map[String, String] =
    super.parsePartitions(dir)
}
