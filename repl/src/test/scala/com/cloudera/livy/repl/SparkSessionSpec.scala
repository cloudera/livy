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

package com.cloudera.livy.repl

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.SparkConf
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods.parse
import org.json4s.JsonAST.JValue
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.rsc.driver.StatementState

class SparkSessionSpec extends BaseSessionSpec {

  override def createInterpreter(): Interpreter =
    new SparkInterpreter(new SparkConf(), new StatementProgressListener(new RSCConf()))

  it should "execute `1 + 2` == 3" in withSession { session =>
    val statement = execute(session)("1 + 2")
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Int = 3"
      )
    ))

    result should equal (expectedResult)
  }

  it should "execute `x = 1`, then `y = 2`, then `x + y`" in withSession { session =>
    val executeWithSession = execute(session)(_)
    var statement = executeWithSession("val x = 1")
    statement.id should equal (0)

    var result = parse(statement.output)
    var expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "x: Int = 1"
      )
    ))

    result should equal (expectedResult)

    statement = executeWithSession("val y = 2")
    statement.id should equal (1)

    result = parse(statement.output)
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 1,
      "data" -> Map(
        "text/plain" -> "y: Int = 2"
      )
    ))

    result should equal (expectedResult)

    statement = executeWithSession("x + y")
    statement.id should equal (2)

    result = parse(statement.output)
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 2,
      "data" -> Map(
        "text/plain" -> "res0: Int = 3"
      )
    ))

    result should equal (expectedResult)
  }

  it should "capture stdout" in withSession { session =>
    val statement = execute(session)("""println("Hello World")""")
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "Hello World"
      )
    ))

    result should equal (expectedResult)
  }

  it should "report an error if accessing an unknown variable" in withSession { session =>
    val statement = execute(session)("""x""")
    statement.id should equal (0)

    val result = parse(statement.output)

    def extract(key: String): String = (result \ key).extract[String]

    extract("status") should equal ("error")
    extract("execution_count") should equal ("0")
    extract("ename") should equal ("Error")
    extract("evalue") should include ("error: not found: value x")
  }

  it should "report an error if exception is thrown" in withSession { session =>
    val statement = execute(session)(
      """def func1() {
        |throw new Exception()
        |}
        |func1()""".stripMargin)
    statement.id should equal (0)

    val result = parse(statement.output)
    val resultMap = result.extract[Map[String, JValue]]

    // Manually extract the values since the line numbers in the exception could change.
    resultMap("status").extract[String] should equal ("error")
    resultMap("execution_count").extract[Int] should equal (0)
    resultMap("ename").extract[String] should equal ("Error")
    resultMap("evalue").extract[String] should include ("java.lang.Exception")

    val traceback = resultMap("traceback").extract[Seq[String]]
    traceback(0) should include ("func1(<console>:")
  }

  it should "access the spark context" in withSession { session =>
    val statement = execute(session)("""sc""")
    statement.id should equal (0)

    val result = parse(statement.output)
    val resultMap = result.extract[Map[String, JValue]]

    // Manually extract the values since the line numbers in the exception could change.
    resultMap("status").extract[String] should equal ("ok")
    resultMap("execution_count").extract[Int] should equal (0)

    val data = resultMap("data").extract[Map[String, JValue]]
    data("text/plain").extract[String] should include (
      "res0: org.apache.spark.SparkContext = org.apache.spark.SparkContext")
  }

  it should "execute spark commands" in withSession { session =>
    val statement = execute(session)(
      """sc.parallelize(0 to 1).map{i => i+1}.collect""".stripMargin)
    statement.id should equal (0)

    val result = parse(statement.output)

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Array[Int] = Array(1, 2)"
      )
    ))

    result should equal (expectedResult)
  }

  it should "do table magic" in withSession { session =>
    val statement = execute(session)("val x = List((1, \"a\"), (3, \"b\"))\n%table x")
    statement.id should equal (0)

    val result = parse(statement.output)

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "application/vnd.livy.table.v1+json" -> Map(
          "headers" -> List(
            Map("type" -> "BIGINT_TYPE", "name" -> "_1"),
            Map("type" -> "STRING_TYPE", "name" -> "_2")),
          "data" -> List(List(1, "a"), List(3, "b"))
        )
      )
    ))

    result should equal (expectedResult)
  }

  it should "cancel spark jobs" in withSession { session =>
    val stmtId = session.execute(
      """sc.parallelize(0 to 10).map { i => Thread.sleep(10000); i + 1 }.collect""".stripMargin)
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(session.statements(stmtId).state.get() == StatementState.Running)
    }
    session.cancel(stmtId)

    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(session.statements(stmtId).state.get() == StatementState.Cancelled)
      session.statements(stmtId).output should include (
        "Job 0 cancelled part of cancelled job group 0")
    }
  }

  it should "cancel waiting statement" in withSession { session =>
    val stmtId1 = session.execute(
      """sc.parallelize(0 to 10).map { i => Thread.sleep(10000); i + 1 }.collect""".stripMargin)
    val stmtId2 = session.execute(
      """sc.parallelize(0 to 10).map { i => Thread.sleep(10000); i + 1 }.collect""".stripMargin)
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(session.statements(stmtId1).state.get() == StatementState.Running)
    }

    assert(session.statements(stmtId2).state.get() == StatementState.Waiting)

    session.cancel(stmtId2)
    assert(session.statements(stmtId2).state.get() == StatementState.Cancelled)

    session.cancel(stmtId1)
    assert(session.statements(stmtId1).state.get() == StatementState.Cancelling)
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(session.statements(stmtId1).state.get() == StatementState.Cancelled)
      session.statements(stmtId1).output should include (
        "Job 0 cancelled part of cancelled job group 0")
    }
  }
}
