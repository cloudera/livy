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

class StatementProgressListenerSpec extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfter
    with LivyBaseUnitTestSuite {
  private val rscConf = new RSCConf()
    .set(RSCConf.Entry.RETAINED_STATEMENT_NUMBER, 2)

  private val testListener = new StatementProgressListener(rscConf) {
    var onJobStartedCallback: Option[() => Unit] = None
    var onJobEndCallback: Option[() => Unit] = None
    var onStageEndCallback: Option[() => Unit] = None
    var onTaskEndCallback: Option[() => Unit] = None

    override  def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      super.onJobStart(jobStart)
      onJobStartedCallback.foreach(f => f())
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      super.onJobEnd(jobEnd)
      onJobEndCallback.foreach(f => f())
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      super.onStageCompleted(stageCompleted)
      onStageEndCallback.foreach(f => f())
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      super.onTaskEnd(taskEnd)
      onTaskEndCallback.foreach(f => f())
    }
  }

  private val statementId = new AtomicInteger(0)

  private def getStatementId = statementId.getAndIncrement()

  private var sparkInterpreter: SparkInterpreter = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkInterpreter = new SparkInterpreter(new SparkConf(), testListener)
    sparkInterpreter.start()
  }

  override def afterAll(): Unit = {
    sparkInterpreter.close()
    super.afterAll()
  }

  after {
    testListener.onJobStartedCallback = None
    testListener.onJobEndCallback = None
    testListener.onStageEndCallback = None
    testListener.onTaskEndCallback = None
  }

  it should "correctly calculate progress" in {
    val executeCode =
      """
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
      """.stripMargin
    val stmtId = getStatementId

    def verifyJobs(): Unit = {
      testListener.statementToJobs.get(stmtId) should not be (None)

      // One job will be submitted
      testListener.statementToJobs(stmtId).size should be (1)
      val jobId = testListener.statementToJobs(stmtId).head.jobId
      testListener.jobIdToStatement(jobId) should be (stmtId)

      // 1 stage will be generated
      testListener.jobIdToStages(jobId).size should be (1)
      val stageIds = testListener.jobIdToStages(jobId)

      // 2 tasks per stage will be generated
      stageIds.foreach { id =>
        testListener.stageIdToTaskCount(id).currFinishedTasks should be (0)
        testListener.stageIdToTaskCount(id).totalTasks should be (2)
      }
    }

    var taskEndCalls = 0
    def verifyTasks(): Unit = {
      taskEndCalls += 1
      testListener.progressOfStatement(stmtId) should be (taskEndCalls.toDouble / 2)
    }

    var stageEndCalls = 0
    def verifyStages(): Unit = {
      stageEndCalls += 1
      testListener.progressOfStatement(stmtId) should be (stageEndCalls.toDouble / 1)
    }

    testListener.onJobStartedCallback = Some(verifyJobs)
    testListener.onTaskEndCallback = Some(verifyTasks)
    testListener.onStageEndCallback = Some(verifyStages)
    sparkInterpreter.execute(stmtId, executeCode)

    eventually(timeout(30 seconds), interval(100 millis)) {
      testListener.progressOfStatement(stmtId) should be(1.0)
    }
  }

  it should "not generate Spark jobs for plain Scala code" in {
    val executeCode = """1 + 1"""
    val stmtId = getStatementId

    def verifyJobs(): Unit = {
      fail("No job will be submitted")
    }

    testListener.onJobStartedCallback = Some(verifyJobs)
    testListener.progressOfStatement(stmtId) should be (0.0)
    sparkInterpreter.execute(stmtId, executeCode)
    testListener.progressOfStatement(stmtId) should be (0.0)
  }

  it should "handle multiple jobs in one statement" in {
    val executeCode =
      """
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
      """.stripMargin
    val stmtId = getStatementId

    var jobs = 0
    def verifyJobs(): Unit = {
      jobs += 1

      testListener.statementToJobs.get(stmtId) should not be (None)
      // One job will be submitted
      testListener.statementToJobs(stmtId).size should be (jobs)
      val jobId = testListener.statementToJobs(stmtId)(jobs - 1).jobId
      testListener.jobIdToStatement(jobId) should be (stmtId)

      // 1 stages will be generated
      testListener.jobIdToStages(jobId).size should be (1)
      val stageIds = testListener.jobIdToStages(jobId)

      // 2 tasks per stage will be generated
      stageIds.foreach { id =>
        testListener.stageIdToTaskCount(id).currFinishedTasks should be (0)
        testListener.stageIdToTaskCount(id).totalTasks should be (2)
      }
    }

    val taskProgress = ArrayBuffer[Double]()
    def verifyTasks(): Unit = {
      taskProgress += testListener.progressOfStatement(stmtId)
    }

    val stageProgress = ArrayBuffer[Double]()
    def verifyStages(): Unit = {
      stageProgress += testListener.progressOfStatement(stmtId)
    }

    testListener.onJobStartedCallback = Some(verifyJobs)
    testListener.onTaskEndCallback = Some(verifyTasks)
    testListener.onStageEndCallback = Some(verifyStages)
    sparkInterpreter.execute(stmtId, executeCode)

    taskProgress.toArray should be (Array(0.5, 1.0, 0.75, 1.0))
    stageProgress.toArray should be (Array(1.0, 1.0))

    eventually(timeout(30 seconds), interval(100 millis)) {
      testListener.progressOfStatement(stmtId) should be(1.0)
    }
  }

  it should "remove old statement progress" in {
    val executeCode =
      """
        |sc.parallelize(1 to 2, 2).map(i => (i, 1)).collect()
      """.stripMargin
    val stmtId = getStatementId

    def onJobEnd(): Unit = {
      testListener.statementToJobs(stmtId).size should be (1)
      testListener.statementToJobs(stmtId).head.isCompleted should be (true)

      testListener.statementToJobs.size should be (2)
      testListener.statementToJobs.get(0) should be (None)
      testListener.jobIdToStatement.filter(_._2 == 0) should be (Map.empty)
    }

    testListener.onJobEndCallback = Some(onJobEnd)
    sparkInterpreter.execute(stmtId, executeCode)
  }
}
