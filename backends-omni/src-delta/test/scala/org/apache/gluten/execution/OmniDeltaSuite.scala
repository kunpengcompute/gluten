/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.Row

/** Omni backend Delta coverage reuses the shared gluten-delta DeltaSuite and adds Omni node checks. */
class OmniDeltaSuite extends DeltaSuite {

  testWithSpecifiedSparkVersion("omni delta: insert and scan offload", Some("3.5")) {
    withTable("omni_delta_insert_scan") {
      spark.sql(
        """
          |CREATE TABLE omni_delta_insert_scan(id BIGINT, name STRING, part STRING)
          |USING DELTA
          |PARTITIONED BY (part)
          |""".stripMargin)
      spark.sql(
        """
          |INSERT INTO omni_delta_insert_scan VALUES
          |(1, 'a', 'p1'),
          |(2, 'b', 'p2'),
          |(3, 'c', 'p1')
          |""".stripMargin)

      val df = runQueryAndCompare("SELECT id, name, part FROM omni_delta_insert_scan WHERE part = 'p1'") {
        _ =>
      }
      val scans = df.queryExecution.executedPlan.collect {
        case scan: OmniDeltaScanExecTransformer => scan
      }
      assert(scans.nonEmpty, "Expected OmniDeltaScanExecTransformer in executed plan")
      checkAnswer(df, Row(1, "a", "p1") :: Row(3, "c", "p1") :: Nil)
    }
  }
}
