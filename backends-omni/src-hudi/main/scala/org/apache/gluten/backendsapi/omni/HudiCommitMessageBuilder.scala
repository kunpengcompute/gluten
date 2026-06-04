/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.connector.write.WriterCommitMessage

import org.apache.gluten.connector.write.HudiFileInfoJson

/**
 * Carries Hudi [[org.apache.hudi.client.WriteStatus]] list from executors to the driver.
 * Driver code ([[org.apache.gluten.execution.OmniHudiInsertIntoCommandExec]]) collects statuses
 * via `getWriteStatuses()`; we use a Gluten-local type so tasks do not depend on
 * `org.apache.spark.sql.hudi.command.HoodieWriterCommitMessage` being on the executor classpath.
 *
 * @since 2026
 */
case class OmniHudiWriterCommitMessage(writeStatuses: java.util.List[Object])
  extends WriterCommitMessage {
  def getWriteStatuses: java.util.List[Object] = writeStatuses
}

/**
 * Builds [[WriterCommitMessage]] from columnar write result (file info JSON array).
 * Wraps reflected [[org.apache.hudi.client.WriteStatus]] instances in [[OmniHudiWriterCommitMessage]].
 *
 * @since 2026
 */
object HudiCommitMessageBuilder {

  private val mapper = new ObjectMapper()

  def buildCommitMessage(fileInfoJsonArray: Array[String]): WriterCommitMessage = {
    if (fileInfoJsonArray == null || fileInfoJsonArray.isEmpty) {
      return OmniHudiWriterCommitMessage(java.util.Collections.emptyList[Object]())
    }
    val writeStatusList = new java.util.ArrayList[Object]()
    for (json <- fileInfoJsonArray) {
      val info = mapper.readValue(json, classOf[HudiFileInfoJson])
      val writeStatus = createWriteStatusFromFileInfo(info)
      if (writeStatus != null) {
        writeStatusList.add(writeStatus)
      }
    }
    OmniHudiWriterCommitMessage(writeStatusList)
  }

  private def createWriteStatusFromFileInfo(info: HudiFileInfoJson): Object = {
    try {
      val writeStatusClass = Class.forName("org.apache.hudi.client.WriteStatus")
      val writeStatus = writeStatusClass.getConstructor().newInstance().asInstanceOf[Object]
      val hoodieWriteStatClass = loadHoodieWriteStatClass()
      val setStat = writeStatusClass.getMethod("setStat", hoodieWriteStatClass)
      val stat = hoodieWriteStatClass.getConstructor().newInstance().asInstanceOf[Object]
      val path = if (info.getPath != null) info.getPath else ""
      hoodieWriteStatClass.getMethod("setPath", classOf[String]).invoke(stat, path)
      invokeIfExists(hoodieWriteStatClass, stat, "setFileId", classOf[String], info.getFileId)
      invokeIfExists(
        hoodieWriteStatClass,
        stat,
        "setPartitionPath",
        classOf[String],
        if (info.getPartitionPath == null) "" else info.getPartitionPath)
      hoodieWriteStatClass.getMethod("setNumWrites", classOf[Long]).invoke(stat, java.lang.Long.valueOf(info.getRecordCount))
      hoodieWriteStatClass.getMethod("setFileSizeInBytes", classOf[Long]).invoke(stat, java.lang.Long.valueOf(info.getFileSizeInBytes))
      setStat.invoke(writeStatus, stat)
      writeStatus
    } catch {
      case _: Throwable => null
    }
  }

  private def invokeIfExists(
      targetClass: Class[_],
      target: Object,
      methodName: String,
      argClass: Class[_],
      value: Object): Unit = {
    if (value == null) {
      return
    }
    try {
      targetClass.getMethod(methodName, argClass).invoke(target, value)
    } catch {
      case _: NoSuchMethodException =>
    }
  }

  private def loadHoodieWriteStatClass(): Class[_] = {
    try {
      Class.forName("org.apache.hudi.common.model.HoodieWriteStat")
    } catch {
      case _: ClassNotFoundException =>
        Class.forName("org.apache.hudi.client.HoodieWriteStat")
    }
  }
}
