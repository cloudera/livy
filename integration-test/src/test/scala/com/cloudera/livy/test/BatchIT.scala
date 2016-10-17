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

import scala.language.postfixOps

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues._

import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.apps._
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class BatchIT extends BaseIntegrationTestSuite with BeforeAndAfterAll {
  private var testLibPath: String = _

  protected override def beforeAll() = {
    super.beforeAll()
    testLibPath = uploadToHdfs(new File(testLib))
  }

  test("submit spark app") {
    val output = newOutputPath()
    withTestLib(classOf[SimpleSparkApp], List(output)) { s =>
      s.verifySessionSuccess()

      // Make sure the test lib has created the test output.
      cluster.fs.isDirectory(new Path(output)) shouldBe true

      // Make sure appInfo is reported correctly.
      val state = s.snapshot()
      state.appInfo.sparkUiUrl.value should startWith ("http")
    }
  }

  test("submit an app that fails") {
    val output = newOutputPath()
    withTestLib(classOf[FailingApp], List(output)) { s =>
      // At this point the application has exited. State should be 'dead' instead of 'error'.
      s.verifySessionDead()

      // The file is written to make sure the app actually ran, instead of just failing for
      // some other reason.
      cluster.fs.isFile(new Path(output)) shouldBe true
    }
  }

  pytest("submit a pyspark application") {
    val scriptPath = uploadResource("batch.py")
    val output = newOutputPath()
    withScript(scriptPath, List(output)) { s =>
      s.verifySessionSuccess()
      cluster.fs.isDirectory(new Path(output)) shouldBe true
    }
  }

  // This is disabled since R scripts don't seem to work in yarn-cluster mode. There's a
  // TODO comment in Spark's ApplicationMaster.scala.
  ignore("submit a SparkR application") {
    val hdfsPath = uploadResource("rtest.R")
    withScript(hdfsPath, List.empty) { s =>
      s.verifySessionSuccess()
    }
  }

  test("deleting a session should kill YARN app") {
    val output = newOutputPath()
    withTestLib(classOf[SimpleSparkApp], List(output, "false")) { s =>
      s.verifySessionState(SessionState.Running())
      s.snapshot().appInfo.driverLogUrl.value should include ("containerlogs")

      val appId = s.appId()

      // Delete the session then verify the YARN app state is KILLED.
      s.stop()
      cluster.yarnClient.getApplicationReport(appId).getFinalApplicationStatus shouldBe
        FinalApplicationStatus.KILLED
    }
  }

  test("killing YARN app should change batch state to dead") {
    val output = newOutputPath()
    withTestLib(classOf[SimpleSparkApp], List(output, "false")) { s =>
      s.verifySessionState(SessionState.Running())
      val appId = s.appId()

      // Kill the YARN app and check batch state should be KILLED.
      cluster.yarnClient.killApplication(appId)
      s.verifySessionDead()

      cluster.yarnClient.getApplicationReport(appId).getFinalApplicationStatus shouldBe
        FinalApplicationStatus.KILLED

      s.snapshot().log should contain ("Application killed by user.")
    }
  }

  test("recover batch sessions") {
    val output1 = newOutputPath()
    val output2 = newOutputPath()
    withTestLib(classOf[SimpleSparkApp], List(output1)) { s1 =>
      s1.stop()
      withTestLib(classOf[SimpleSparkApp], List(output2, "false")) { s2 =>
        s2.verifySessionRunning()

        // Restart Livy.
        cluster.stopLivy()
        cluster.runLivy()

        // Verify previous active session still appears after restart.
        s2.verifySessionRunning()
        // Verify deleted session doesn't show up.
        s1.verifySessionDoesNotExist()

        s2.stop()

        // Verify new session doesn't reuse old session id.
        withTestLib(classOf[SimpleSparkApp], List(output2)) { s3 =>
          s3.id should be > s2.id
        }
      }
    }
  }

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

  private def withScript[R]
    (scriptPath: String, args: List[String], sparkConf: Map[String, String] = Map.empty)
    (f: (LivyRestClient#BatchSession) => R): R = {
    val s = livyClient.startBatch(scriptPath, None, args, sparkConf)
    withSession(s)(f)
  }

  private def withTestLib[R]
    (testClass: Class[_], args: List[String], sparkConf: Map[String, String] = Map.empty)
    (f: (LivyRestClient#BatchSession) => R): R = {
    val s = livyClient.startBatch(testLibPath, Some(testClass.getName()), args, sparkConf)
    withSession(s)(f)
  }
}
