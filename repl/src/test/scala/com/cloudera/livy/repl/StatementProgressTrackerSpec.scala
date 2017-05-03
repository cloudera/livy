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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{postfixOps, reflectiveCalls}

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.scalatest._
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.LivyBaseUnitTestSuite
import com.cloudera.livy.rsc.RSCConf

class StatementProgressTrackerSpec extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with LivyBaseUnitTestSuite {

  private val statementId = new AtomicInteger(0)

  private def getStatementId = statementId.getAndIncrement()

  private var sparkInterpreter: SparkInterpreter = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkInterpreter = new SparkInterpreter(new SparkConf())
    sparkInterpreter.start()
  }

  override def afterAll(): Unit = {
    sparkInterpreter.close()
    super.afterAll()
  }

  it should "correctly calculate progress" in {
    val executeCode =
      """
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
      """.stripMargin
    val stmtId = getStatementId

    sparkInterpreter.statementProgressTracker should not be (None)

    sparkInterpreter.execute(stmtId, executeCode)
    eventually(timeout(30 seconds), interval(100 millis)) {
      sparkInterpreter.statementProgressTracker.get.progressOfStatement(stmtId) should be(1.0)
    }
  }

  it should "not generate Spark jobs for plain Scala code" in {
    val executeCode = """1 + 1"""
    val stmtId = getStatementId

    sparkInterpreter.statementProgressTracker.get.progressOfStatement(stmtId) should be (0.0)
    sparkInterpreter.execute(stmtId, executeCode)
    sparkInterpreter.statementProgressTracker.get.progressOfStatement(stmtId) should be (0.0)
  }

  it should "handle multiple jobs in one statement" in {
    val executeCode =
      """
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
      """.stripMargin
    val stmtId = getStatementId

    sparkInterpreter.statementProgressTracker should not be (None)

    sparkInterpreter.execute(stmtId, executeCode)
    eventually(timeout(30 seconds), interval(100 millis)) {
      sparkInterpreter.statementProgressTracker.get.progressOfStatement(stmtId) should be(1.0)
    }
  }
}
