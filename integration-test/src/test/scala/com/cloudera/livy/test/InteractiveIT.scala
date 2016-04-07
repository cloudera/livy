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
import java.util.regex.Pattern

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.sessions._
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, StatementError}

class InteractiveIT extends BaseIntegrationTestSuite with BeforeAndAfter {
  private var sessionId: Int = -1

  after {
    livyClient.stopSession(sessionId)
    sessionId = -1
  }

  test("basic interactive session") {
    sessionId = livyClient.startSession(Spark())

    matchResult("1+1", "res0: Int = 2")
    matchResult("sqlContext", startsWith("res1: org.apache.spark.sql.hive.HiveContext"))
    matchResult("val sql = new org.apache.spark.sql.SQLContext(sc)",
      startsWith("sql: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext"))

    matchError("abcde", evalue = ".*?:[0-9]+: error: not found: value abcde.*")
    matchError("throw new IllegalStateException()",
      evalue = ".*java\\.lang\\.IllegalStateException.*")
  }

  pytest("pyspark interactive session") {
    sessionId = livyClient.startSession(PySpark())

    matchResult("1+1", "2")
    matchResult("sqlContext", startsWith("<pyspark.sql.context.HiveContext"))
    matchResult("sc.parallelize(range(100)).map(lambda x: x * 2).reduce(lambda x, y: x + y)",
      "9900")

    matchError("abcde", ename = "NameError", evalue = "name 'abcde' is not defined")
    matchError("raise KeyError, 'foo'", ename = "KeyError", evalue = "'foo'")
  }

  rtest("R interactive session") {
    sessionId = livyClient.startSession(SparkR())

    // R's output sometimes includes the count of statements, which makes it annoying to test
    // things. This helps a bit.
    val curr = new AtomicInteger()
    def count: Int = curr.incrementAndGet()

    matchResult("1+1", startsWith(s"[$count] 2"))
    matchResult("sqlContext <- sparkRSQL.init(sc)", null)
    matchResult("hiveContext <- sparkRHive.init(sc)", null)
    matchResult("""localDF <- data.frame(name=c("John", "Smith", "Sarah"), age=c(19, 23, 18))""",
      null)
    matchResult("df <- createDataFrame(sqlContext, localDF)", null)
    matchResult("printSchema(df)", literal(
      """|root
         | |-- name: string (nullable = true)
         | |-- age: double (nullable = true)""".stripMargin))
  }

  test("application kills session") {
    sessionId = livyClient.startSession(Spark())
    waitTillSessionIdle(sessionId)
    livyClient.runStatement(sessionId, "System.exit(0)")

    val expected = Set(SessionState.Idle().toString, SessionState.Error().toString)
    eventually(timeout(30 seconds), interval(1 second)) {
      val state = livyClient.getSessionStatus(sessionId)
      assert(expected.contains(state))
    }

    // After the statement has run, it shouldn't be possible to run more commands. Once LIVY-139
    // is fixed, this test should be changed to make sure the session state automatically turns
    // to "error" or "dead", depending on how it's implemented.
    try {
      livyClient.runStatement(sessionId, "1+1")
      val state = livyClient.getSessionStatus(sessionId)
      fail(s"Should have failed to run statement; session state is $state")
    } catch {
      case e: Exception =>
    }
  }

  test("user jars are properly imported in Scala interactive sessions") {
    // Include a popular Java library to test importing user jars.
    sessionId = livyClient.startSession(
      Spark(),
      Map("spark.jars.packages" -> "org.codehaus.plexus:plexus-utils:3.0.24"))

    // Check is the library loaded in JVM in the proper class loader.
    matchResult("Thread.currentThread.getContextClassLoader.loadClass" +
      """("org.codehaus.plexus.util.FileUtils")""",
      ".*Class\\[_\\] = class org.codehaus.plexus.util.FileUtils")

    // Check does Scala interpreter see the library.
    matchResult("import org.codehaus.plexus.util._", "import org.codehaus.plexus.util._")

    // Check does SparkContext see classes defined by Scala interpreter.
    matchResult("case class Item(i: Int)", "defined class Item")
    matchResult(
      "val rdd = sc.parallelize(Array.fill(10){new Item(scala.util.Random.nextInt(1000))})",
      "rdd.*")
    matchResult("rdd.count()", ".*= 10")
  }

  private def matchResult(code: String, expected: String): Unit = {
    runAndValidateStatement(code) match {
      case Left(result) =>
        if (expected != null) {
          matchStrings(result, expected)
        }

      case Right(error) =>
        fail(s"Got error from statement $code: ${error.evalue}")
    }
  }

  private def matchError(
      code: String,
      ename: String = null,
      evalue: String = null,
      stackTrace: String = null): Unit = {
    runAndValidateStatement(code) match {
      case Left(result) =>
        fail(s"Statement `$code` expected to fail, but succeeded.")

      case Right(error) =>
        val remoteStack = Option(error.stackTrace).getOrElse(Nil).mkString("\n")
        Seq(
          error.ename -> ename,
          error.evalue -> evalue,
          remoteStack -> stackTrace
        ).foreach { case (actual, expected) =>
          if (expected != null) {
            matchStrings(actual, expected)
          }
        }
    }
  }

  private def matchStrings(actual: String, expected: String): Unit = {
    val regex = Pattern.compile(expected, Pattern.DOTALL)
    // Don't use assert to make the error message easier to read.
    if (!regex.matcher(actual).matches()) {
      fail(s"$actual did not match regex $expected")
    }
  }

  private def startsWith(result: String): String = Pattern.quote(result) + ".*"

  private def literal(result: String): String = Pattern.quote(result)

  private def runAndValidateStatement(code: String): Either[String, StatementError] = {
    waitTillSessionIdle(sessionId)
    val stmtId = livyClient.runStatement(sessionId, code)
    waitTillSessionIdle(sessionId)
    livyClient.getStatementResult(sessionId, stmtId)
  }

}
