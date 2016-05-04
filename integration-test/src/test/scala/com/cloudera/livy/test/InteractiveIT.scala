/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.test

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter

import com.cloudera.livy.sessions._
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

private case class TestStatement(
  stmt: String,
  expectedResult: Option[String],
  var stmtId: Int = -1)

class InteractiveIT extends BaseIntegrationTestSuite with BeforeAndAfter {

  private var sessionId: Int = -1

  after {
    livyClient.stopSession(sessionId)
    sessionId = -1
  }

  test("basic interactive session") {
    sessionId = livyClient.startSession(Spark())

    val testStmts = List(
      new TestStatement("1+1", Some("res0: Int = 2")),
      new TestStatement("val sqlContext = new org.apache.spark.sql.SQLContext(sc)",
        Some("sqlContext: org.apache.spark.sql.SQLContext = " +
          "org.apache.spark.sql.SQLContext")))
    runAndValidateStatements(testStmts)
  }

  pytest("pyspark interactive session") {
    sessionId = livyClient.startSession(PySpark())

    val testStmts = List(
      new TestStatement("1+1", Some("2")),
      new TestStatement(
        "sc.parallelize(range(100)).map(lambda x: x * 2).reduce(lambda x, y: x + y)",
        Some("9900")))
    runAndValidateStatements(testStmts)
  }

  rtest("R interactive session") {
    sessionId = livyClient.startSession(SparkR())

    // R's output sometimes includes the count of statements, which makes it annoying to test
    // things. This helps a bit.
    val curr = new AtomicInteger()
    def count: Int = curr.incrementAndGet()

    val testStmts = List(
      new TestStatement("1+1", Some(s"[$count] 2")),
      new TestStatement("sqlContext <- sparkRSQL.init(sc)", None),
      new TestStatement(
        """localDF <- data.frame(name=c("John", "Smith", "Sarah"), age=c(19, 23, 18))""", None),
      new TestStatement("df <- createDataFrame(sqlContext, localDF)", None),
      new TestStatement("printSchema(df)", Some(
      """|root
         | |-- name: string (nullable = true)
         | |-- age: double (nullable = true)""".stripMargin))
    )
    runAndValidateStatements(testStmts)
  }

  private def runAndValidateStatements(statements: Seq[TestStatement]) = {
    waitTillSessionIdle(sessionId)
    statements.foreach(runAndValidateStatement)
  }

  private def runAndValidateStatement(testStmt: TestStatement) = {
    testStmt.stmtId = livyClient.runStatement(sessionId, testStmt.stmt)

    waitTillSessionIdle(sessionId)

    testStmt.expectedResult.map { s =>
      val result = livyClient.getStatementResult(sessionId, testStmt.stmtId)
      assert(result.contains(s))
    }

  }

}
