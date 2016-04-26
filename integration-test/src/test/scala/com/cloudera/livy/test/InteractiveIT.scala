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

import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter

import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

private case class TestStatement(
  stmt: String,
  expectedResult: Option[String],
  var stmtId: Int = -1)

class InteractiveIT extends BaseIntegrationTestSuite with BeforeAndAfter {

  private var sessionId: Int = -1

  after {
    livyClient.stopSession(sessionId)
  }

  test("basic interactive session") {
    sessionId = livyClient.startSession()

    val testStmts = List(
      new TestStatement("1+1", Some("res0: Int = 2")),
      new TestStatement("val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)",
        Some("hiveContext: org.apache.spark.sql.hive.HiveContext = " +
          "org.apache.spark.sql.hive.HiveContext")))

    waitTillSessionIdle(sessionId)

    // Run the statements
    testStmts.foreach {
      runAndValidateStatement(_)
    }
  }

  private def runAndValidateStatement(testStmt: TestStatement) = {
    testStmt.stmtId = livyClient.runStatement(sessionId, testStmt.stmt)

    waitTillSessionIdle(sessionId)

    testStmt.expectedResult.map { s =>
      val result = livyClient.getStatementResult(sessionId, testStmt.stmtId)
      assert(result.indexOf(s) >= 0,
        s"Statement result doesn't match. Expected: $s. Actual: $result")
    }

  }

}
