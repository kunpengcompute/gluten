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
package org.apache.spark.sql.hive

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer, GenericExpressionTransformer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

object OmniHiveUDFTransformer {
  def replaceWithExpressionTransformer(
                                        expr: Expression,
                                        attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val udfName = expr match {
      case s: HiveSimpleUDF =>
        "HiveSimpleUDF#" + s.name.stripPrefix("default.")
      case g: HiveGenericUDF =>
        g.name.stripPrefix("default.")
      case _ =>
        throw new GlutenNotSupportException(
          s"Expression $expr is not a HiveSimpleUDF or HiveGenericUDF")
    }
    genTransformerFromUDFMappings(udfName, expr, attributeSeq)
  }

  def genTransformerFromUDFMappings(
                                     udfName: String,
                                     expr: Expression,
                                     attributeSeq: Seq[Attribute]): GenericExpressionTransformer = {
    GenericExpressionTransformer(
      udfName,
      ExpressionConverter.replaceWithExpressionTransformer(expr.children, attributeSeq),
      expr)
  }
}
