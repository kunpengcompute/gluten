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
package org.apache.gluten.execution

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.FileFormat

/**
 * Heuristic detection of Apache Hudi read paths so `_hoodie_*` names are only treated as Hudi
 * metadata/virtual columns when the scan is actually Hudi-backed (avoids user columns that happen
 * to use the same prefix on non-Hudi tables).
 */
object HudiDatasourceDetection {

  /** Spark V1 [[FileFormat]] from Hudi bundles uses class names containing `Hoodie`. */
  def isHudiSparkFileFormat(fileFormat: FileFormat): Boolean =
    fileFormat != null && fileFormat.getClass.getName.indexOf("Hoodie") >= 0

  /** DSv2 [[Scan]] implementations from Hudi typically contain `Hoodie` in the class name. */
  def isHudiConnectorScan(scan: Scan): Boolean =
    scan != null && scan.getClass.getName.indexOf("Hoodie") >= 0
}
