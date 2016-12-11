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
import org.scalatest._
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.rsc.driver.StatementState
import com.cloudera.livy.sessions._

abstract class PythonSessionSpec extends BaseSessionSpec {

  it should "execute `1 + 2` == 3" in withSession { session =>
    val statement = execute(session)("1 + 2")
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "3"
      )
    ))

    result should equal (expectedResult)
  }

  it should "execute `x = 1`, then `y = 2`, then `x + y`" in withSession { session =>
    val executeWithSession = execute(session)(_)
    var statement = executeWithSession("x = 1")
    statement.id should equal (0)

    var result = parse(statement.output)
    var expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> ""
      )
    ))

    result should equal (expectedResult)

    statement = executeWithSession("y = 2")
    statement.id should equal (1)

    result = parse(statement.output)
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 1,
      "data" -> Map(
        "text/plain" -> ""
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
        "text/plain" -> "3"
      )
    ))

    result should equal (expectedResult)
  }

  it should "do table magic" in withSession { session =>
    val statement = execute(session)("x = [[1, 'a'], [3, 'b']]\n%table x")
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "application/vnd.livy.table.v1+json" -> Map(
          "headers" -> List(
            Map("type" -> "INT_TYPE", "name" -> "0"),
            Map("type" -> "STRING_TYPE", "name" -> "1")),
          "data" -> List(List(1, "a"), List(3, "b"))
        )
      )
    ))

    result should equal (expectedResult)
  }

  it should "capture stdout" in withSession { session =>
    val statement = execute(session)("""print('Hello World')""")
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
    val expectedResult = Extraction.decompose(Map(
      "status" -> "error",
      "execution_count" -> 0,
      "traceback" -> List(
        "Traceback (most recent call last):\n",
        "NameError: name 'x' is not defined\n"
      ),
      "ename" -> "NameError",
      "evalue" -> "name 'x' is not defined"
    ))

    result should equal (expectedResult)
  }

  it should "report an error if exception is thrown" in withSession { session =>
    val statement = execute(session)(
      """def func1():
        |  raise Exception("message")
        |def func2():
        |  func1()
        |func2()
      """.stripMargin)
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "error",
      "execution_count" -> 0,
      "traceback" -> List(
        "Traceback (most recent call last):\n",
        "  File \"<stdin>\", line 4, in func2\n",
        "  File \"<stdin>\", line 2, in func1\n",
        "Exception: message\n"
      ),
      "ename" -> "Exception",
      "evalue" -> "message"
    ))

    result should equal (expectedResult)
  }

  it should "cancel pyspark jobs" in withSession { session =>
    val stmtId = session.execute(
      """
        |def func(i):
        |  from time import sleep
        |  sleep(10)
        |  return i
        |sc.range(10).map(func).collect()
      """.stripMargin)

    session.cancel(stmtId)

    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(session.statements(stmtId).state.get() == StatementState.Cancelled)
    }
  }
}

class Python2SessionSpec extends PythonSessionSpec {
  override def createInterpreter(): Interpreter = PythonInterpreter(new SparkConf(), PySpark())
}

class Python3SessionSpec extends PythonSessionSpec {

  override protected def withFixture(test: NoArgTest): Outcome = {
    assume(!sys.props.getOrElse("skipPySpark3Tests", "false").toBoolean, "Skipping PySpark3 tests.")
    test()
  }

  override def createInterpreter(): Interpreter = PythonInterpreter(new SparkConf(), PySpark3())

  it should "check python version is 3.x" in withSession { session =>
    val statement = execute(session)(
      """import sys
      |sys.version >= '3'
      """.stripMargin)
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "True"
      )
    ))

    result should equal (expectedResult)
  }
}
