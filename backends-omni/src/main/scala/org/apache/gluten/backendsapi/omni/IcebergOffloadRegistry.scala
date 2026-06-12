/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

/**
 * Loads Iceberg scan and write offload rules via reflection; present only when -Piceberg and
 * src-iceberg are compiled.
 */
object IcebergOffloadRegistry {

  def offloads: Seq[OffloadSingleNode] = loadScanOffloads() ++ loadWriteOffloads()

  private def loadScanOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.execution.OffloadIcebergScan")
      val ctor = clazz.getConstructor()
      Seq(ctor.newInstance().asInstanceOf[OffloadSingleNode])
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadWriteOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadIcebergWrite")
      val method = clazz.getMethod("offloads")
      method.invoke(null).asInstanceOf[Seq[OffloadSingleNode]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }
}
