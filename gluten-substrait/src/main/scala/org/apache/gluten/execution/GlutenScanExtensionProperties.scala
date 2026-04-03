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

/**
 * Optional scan [[org.apache.gluten.execution.BasicScanExecTransformer.getProperties]] keys for
 * datasource-specific wiring. Unknown backends ignore entries they do not consume.
 *
 * Hudi: driver sets [[HudiDataSourceProperty]] when the scan is Hudi-backed; Omni native code may
 * read the marker to treat `_hoodie_*` as virtual columns only in that case.
 */
object GlutenScanExtensionProperties {

  /** Value is typically `"true"`. String values are stable for JNI / persisted plans. */
  val HudiDataSourceProperty: String = "gluten.omni.scan.hudiDataSource"

  /**
   * Internal metadata entry appended per file (substrait `metadata_columns` → native split info).
   * Not a Spark output column.
   */
  val InternalHudiDatasourceMetadataKey: String = "__gluten_omni_internal__.hudi_datasource"
}
