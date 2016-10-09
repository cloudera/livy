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
import java.util.UUID
import javax.servlet.http.HttpServletResponse._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.Exception.{allCatch, ignoring, Catch}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus}
import org.apache.hadoop.yarn.util.ConverterUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.apps._
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

class BatchIT extends BaseIntegrationTestSuite with BeforeAndAfterAll {
  val DRIVER_LOG_URL_NAME = "driverLogUrl"
  val SPARK_UI_URL_NAME = "sparkUiUrl"

  implicit val patienceConfig = PatienceConfig(timeout = 2 minutes, interval = 1 second)

  private var testLibPath: String = _

  protected override def beforeAll() = {
    super.beforeAll()
    testLibPath = uploadToHdfs(new File(testLib))
  }

  test("submit spark app") {
    val output = newOutputPath()
    val result = runSpark(classOf[SimpleSparkApp], args = List(output))

    dumpLogOnFailure(result.id) {
      assert(result.state === SessionState.Success().toString())
      assert(cluster.fs.isDirectory(new Path(output)))
      result.appInfo should contain key DRIVER_LOG_URL_NAME
      result.appInfo should contain key SPARK_UI_URL_NAME
      result.appInfo.get(DRIVER_LOG_URL_NAME) should include ("containerlogs")
      result.appInfo.get(SPARK_UI_URL_NAME) should startWith ("http")

    }
  }

  test("submit an app that fails") {
    val output = newOutputPath()
    val result = runSpark(classOf[FailingApp], args = List(output))
    // At this point the application has exited. State should be 'dead' instead of 'error'.
    assert(result.state === SessionState.Dead().toString())

    // The file is written to make sure the app actually ran, instead of just failing for
    // some other reason.
    assert(cluster.fs.isFile(new Path(output)))
  }

  pytest("submit a pyspark application") {
    val hdfsPath = uploadResource("batch.py")
    val output = newOutputPath()
    val result = runScript(hdfsPath, args = List(output))
    assert(result.state === SessionState.Success().toString())
    assert(cluster.fs.isDirectory(new Path(output)))
  }

  // This is disabled since R scripts don't seem to work in yarn-cluster mode. There's a
  // TODO comment in Spark's ApplicationMaster.scala.
  ignore("submit a SparkR application") {
    val hdfsPath = uploadResource("rtest.R")
    val result = runScript(hdfsPath)
    assert(result.state === SessionState.Success().toString())
  }

  test("deleting a session should kill YARN app") {
    val output = newOutputPath()
    val batchId = runSpark(classOf[SimpleSparkApp], List(output, "false"), waitForExit = false).id

    dumpLogOnFailure(batchId) {
      val appId = waitUntilRunning(batchId)

      // Delete the session then verify the YARN app state is KILLED.
      deleteBatch(batchId)
      assert(cluster.yarnClient.getApplicationReport(appId)
        .getFinalApplicationStatus == FinalApplicationStatus.KILLED,
        "YARN app should be killed.")
    }
  }

  test("killing YARN app should change batch state to dead") {
    val output = newOutputPath()
    val batchId = runSpark(classOf[SimpleSparkApp], List(output, "false"), waitForExit = false).id

    dumpLogOnFailure(batchId) {
      val appId = waitUntilRunning(batchId)

      // Kill the YARN app and check batch state should be KILLED.
      cluster.yarnClient.killApplication(appId)
      eventually {
        assert(cluster.yarnClient.getApplicationReport(appId)
          .getFinalApplicationStatus == FinalApplicationStatus.KILLED,
          "YARN app should be killed.")
        val batchInfo = getBatchSessionInfo(batchId)
        assert(batchInfo.state == SessionState.Dead().toString(), "Batch state should be dead.")
        assert(batchInfo.log.contains("Application killed by user."),
          "Batch log doesn't contain yarn final diagnostics.")
      }
    }
  }

  private def dumpLogOnFailure[T](batchId: Int, cleanup: Boolean = true)(f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable =>
        ignoringAll {
          info(s"Session log: ${getBatchSessionInfo(batchId).log}")
          info(s"YARN log: ${getBatchYarnLog(batchId)}")
        }
        throw e
    } finally {
      ignoringAll { deleteBatch(batchId) }
    }
  }

  private def getBatchYarnLog(batchId: Int): String = {
    allCatch.opt {
      val appId = getBatchSessionInfo(batchId).appId
      assert(appId != null, "appId shouldn't be null")

      getYarnLog(appId)
    }.getOrElse("")
  }

  private def ignoringAll: Catch[Unit] = ignoring(classOf[Throwable])

  private def newOutputPath(): String = {
    cluster.hdfsScratchDir().toString() + "/" + UUID.randomUUID().toString()
  }

  private def uploadResource(name: String): String = {
    val hdfsPath = new Path(cluster.hdfsScratchDir(), UUID.randomUUID().toString + "-" + name)
    val in = getClass.getResourceAsStream("/" + name)
    val out = cluster.fs.create(hdfsPath)
    try {
      IOUtils.copy(in, out)
    } finally {
      in.close()
      out.close()
    }
    hdfsPath.toUri().getPath()
  }

  class MockCreateBatchRequest {
    var file: String = _
    var args: List[String] = List()
    var className: Option[String] = None
    var conf: Map[String, String] = Map()
  }
  private def runScript(script: String, args: List[String] = Nil): SessionInfo = {
    val request = new MockCreateBatchRequest()
    request.file = script
    request.args = args
    waitAndDeleteBatch(startBatch(request).id)
  }

  private def runSpark(
      klass: Class[_],
      args: List[String] = Nil,
      waitForExit: Boolean = true): SessionInfo = {
    val request = new MockCreateBatchRequest()
    request.file = testLibPath
    request.className = Some(klass.getName)
    request.args = args

    val batchId = startBatch(request)
    if (waitForExit) waitAndDeleteBatch(batchId.id) else batchId
  }

  private def getBatchSessionInfo(batchId: Int): SessionInfo = {
    val r = httpClient.prepareGet(s"$livyEndpoint/batches/$batchId")
      .execute()
      .get()
    assert(r.getStatusCode() === SC_OK)
    mapper.readValue(r.getResponseBodyAsStream(), classOf[SessionInfo])
  }

  private def deleteBatch(batchId: Int): Unit = {
    val r = httpClient.prepareDelete(s"$livyEndpoint/batches/$batchId")
      .execute()
      .get()
    assert(r.getStatusCode() === SC_OK)
  }

  private def startBatch(request: MockCreateBatchRequest): SessionInfo = {
    request.conf = Map("spark.yarn.maxAppAttempts" -> "1")

    val response = httpClient.preparePost(s"$livyEndpoint/batches")
      .setBody(mapper.writeValueAsString(request))
      .execute()
      .get()

    withClue(response) {
      assert(response.getStatusCode === SC_CREATED)
    }

    mapper.readValue(response.getResponseBodyAsStream(), classOf[SessionInfo])
  }

  private def waitUntilRunning(batchId: Int): ApplicationId = {
    eventually {
      val batch = getBatchSessionInfo(batchId)

      // If batch state transits to any error state, fail test immediately.
      if (batch.state == SessionState.Error().toString() ||
        batch.state == SessionState.Dead().toString()) {
        fail(s"Session shouldn't be in a terminal state: ${batch.state}")
      }

      assert(batch.state === SessionState.Running().toString())

      ConverterUtils.toApplicationId(batch.appId)
    }
  }

  private def waitAndDeleteBatch(batchId: Int): SessionInfo = {
    val terminalStates = Set(SessionState.Error(), SessionState.Dead(), SessionState.Success())
      .map(_.toString())

    var finished = false
    try {
      eventually {
        val r = getBatchSessionInfo(batchId)
        assert(terminalStates.contains(r.state))

        finished = true
        r
      }
    } finally {
      if (!finished) {
        deleteBatch(batchId)
      }
    }
  }
}
