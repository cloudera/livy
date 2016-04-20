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

import java.io.File
import javax.servlet.http.HttpServletResponse

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter

import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.sessions.{SessionKindModule, SessionState}
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, FatalException}

private case class TestStatement(
  stmt: String,
  expectedResult: Option[String],
  var stmtId: Int = -1)

class InteractiveIT extends BaseIntegrationTestSuite with BeforeAndAfter {

  private var sessionId: Int = -1

  after {
    if (sessionId != -1) {
      httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
      sessionId = -1
    }
  }

  it("basic interactive session") {
    sessionId = livyClient.startInteractiveSession()

    val testStmts = List(
      new TestStatement("1+1", Some("res0: Int = 2")),
      new TestStatement("val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)",
        Some("hiveContext: org.apache.spark.sql.hive.HiveContext = " +
          "org.apache.spark.sql.hive.HiveContext")))

    waitTillSessionIdle()

    // Run the statements
    testStmts.foreach {
      runAndValidateStatement(_)
    }
  }

  private def runAndValidateStatement(testStmt: TestStatement) = {
    testStmt.stmtId = livyClient.runStatementInSession(sessionId, testStmt.stmt)

    waitTillSessionIdle()

    testStmt.expectedResult.map { s =>
      val result = livyClient.getStatementResult(sessionId, testStmt.stmtId)
      if (result.indexOf(s) == -1) {
        throw new FatalException(s"Statement result doesn't match. Expected: $s. Actual: $result")
      }
    }

  }

  @tailrec
  private def waitTillSessionIdle(): Unit = {
    val curState = livyClient.getInteractivelStatus(sessionId)
    val terminalStates = Set(SessionState.Success().toString, SessionState.Dead().toString,
      SessionState.Error().toString)

    assert(!terminalStates.contains(curState),
      s"Session is in unexpected terminal state $curState.")

    if (curState != SessionState.Idle().toString) {
      Thread.sleep(1.second.toMillis)
      waitTillSessionIdle()
    }
  }

}
