/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ValidationResult

import org.apache.iceberg.{FileFormat, PartitionField, PartitionSpec, Schema, TableProperties}
import org.apache.iceberg.TableProperties.{
  ORC_COMPRESSION,
  ORC_COMPRESSION_DEFAULT,
  PARQUET_COMPRESSION,
  PARQUET_COMPRESSION_DEFAULT
}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.spark.source.IcebergWriteUtil

import scala.collection.JavaConverters._

/**
 * Iceberg columnar write Exec validation: extends ColumnarV2TableWriteExec; doValidateInternal
 * checks format (Parquet/ORC only), codec, partition types, no sort order, no merge schema, etc.,
 * so the backend can safely use columnar write.
 */
trait IcebergWriteExec extends ColumnarV2TableWriteExec {

  /** Converts file format enum to int: PARQUET=1, ORC=0 */
  protected def getFileFormat(format: FileFormat): Int = {
    format match {
      case FileFormat.PARQUET => 1
      case FileFormat.ORC => 0
      case _ => throw new UnsupportedOperationException()
    }
  }

  /** Gets compression codec from Iceberg write properties; "uncompressed" is converted to "none". */
  protected def getCodec: String = {
    val config = IcebergWriteUtil.getWriteProperty(write)
    val codec = IcebergWriteUtil.getFileFormat(write) match {
      case FileFormat.PARQUET =>
        config.getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT)
      case FileFormat.ORC => config.getOrDefault(ORC_COMPRESSION, ORC_COMPRESSION_DEFAULT)
      case _ => throw new UnsupportedOperationException()
    }
    if (codec.equalsIgnoreCase("uncompressed")) {
      "none"
    } else {
      codec
    }
  }

  /** Partition spec (from IcebergWriteUtil using table and writeConf). */
  protected def getPartitionSpec: PartitionSpec = {
    IcebergWriteUtil.getPartitionSpec(write)
  }

  private def validatePartitionType(schema: Schema, field: PartitionField): Boolean = {
    val partitionType = schema.findType(field.sourceId())
    partitionType.isPrimitiveType
  }

  /** Validates: write type, data types, partition types, format, codec, column names, acceptAnySchema, mergeSchema, etc. */
  override def doValidateInternal(): ValidationResult = {
    if (!IcebergWriteUtil.supportsWrite(write)) {
      return ValidationResult.failed(s"Not support the write ${write.getClass.getSimpleName}")
    }
    if (IcebergWriteUtil.hasUnsupportedDataType(write)) {
      return ValidationResult.failed("Contains UUID or FIXED data type")
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    val spec = IcebergWriteUtil.getTable(write).spec()
    if (spec.isPartitioned) {
      val topIds = spec.schema().columns().asScala.map(c => c.fieldId())
      if (
        spec
          .fields()
          .stream()
          .anyMatch(
            f =>
              !validatePartitionType(spec.schema(), f) || !topIds
                .contains(f.sourceId()) || f.transform().isVoid)
      ) {
        return ValidationResult.failed(
          "Not support write unsupported partition type, or is nested partition column")
      }
    }
    if (IcebergWriteUtil.getTable(write).sortOrder().isSorted) {
      return ValidationResult.failed("Not support write table with sort order")
    }
    val format = IcebergWriteUtil.getFileFormat(write)
    if (format != FileFormat.PARQUET && format != FileFormat.ORC) {
      return ValidationResult.failed("Not support this format " + format.name())
    }

    val codec = getCodec
    if (Seq("brotli", "lzo").contains(codec)) {
      return ValidationResult.failed("Not support this codec " + codec)
    }
    if (query.output.exists(a => !AvroSchemaUtil.makeCompatibleName(a.name).equals(a.name))) {
      return ValidationResult.failed("Not support the compatible column name")
    }
    if (
      IcebergWriteUtil
        .getTable(write)
        .properties()
        .getOrDefault(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, "false")
        .equals("true")
    ) {
      return ValidationResult.failed("Not support the write with accept any schema")
    }
    if (IcebergWriteUtil.getWriteConf(write).mergeSchema()) {
      return ValidationResult.failed("Not support write with merge schema")
    }

    ValidationResult.succeeded
  }
}
