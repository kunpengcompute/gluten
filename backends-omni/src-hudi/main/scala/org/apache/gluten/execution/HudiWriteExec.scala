/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.omni.HudiWriteUtil
import org.apache.gluten.extension.ValidationResult

/**
 * Hudi columnar write Exec validation (aligned with IcebergWriteExec):
 * extends ColumnarV2TableWriteExec; doValidateInternal checks Hudi write and schema.
 *
 * @since 2026
 */

trait HudiWriteExec extends ColumnarV2TableWriteExec {

  /**
   * Validates that the write is a supported Hudi [[org.apache.spark.sql.connector.write.Write]]
   * and that the child output schema is offloadable by the Omni validator.
   */
  override def doValidateInternal(): ValidationResult = {
    if (!HudiWriteUtil.supportsWrite(write)) {
      return ValidationResult.failed(s"Not support the write ${write.getClass.getSimpleName}")
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }
}
