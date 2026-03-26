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

import org.apache.spark.sql.Row

class OmniDeltaSuite extends DeltaSuite {

  testWithSpecifiedSparkVersion("delta 3.3: create insert read with order by", Some("3.3")) {
    withTable("t_delta_users") {
      spark.sql(
        """
          |CREATE TABLE t_delta_users (
          |  id INT,
          |  name STRING,
          |  age INT
          |) USING delta
          |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t_delta_users VALUES
          |  (3, 'cathy', 30),
          |  (1, 'alice', 18),
          |  (2, 'bob', 24)
          |""".stripMargin)

      val df = runQueryAndCompare("SELECT * FROM t_delta_users ORDER BY id") { _ => }
      checkLengthAndPlan(df, 3)
      checkAnswer(df, Seq(Row(1, "alice", 18), Row(2, "bob", 24), Row(3, "cathy", 30)))
    }
  }

  testWithSpecifiedSparkVersion("delta 3.5: create insert read with order by", Some("3.5")) {
    withTable("t_delta_users") {
      spark.sql(
        """
          |CREATE TABLE t_delta_users (
          |  id INT,
          |  name STRING,
          |  age INT
          |) USING delta
          |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t_delta_users VALUES
          |  (3, 'cathy', 30),
          |  (1, 'alice', 18),
          |  (2, 'bob', 24)
          |""".stripMargin)

      val df = runQueryAndCompare("SELECT * FROM t_delta_users ORDER BY id") { _ => }
      checkLengthAndPlan(df, 3)
      checkAnswer(df, Seq(Row(1, "alice", 18), Row(2, "bob", 24), Row(3, "cathy", 30)))
    }
  }
}
