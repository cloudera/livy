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

package com.cloudera.livy.server.interactive

import java.net.URI

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.launcher.SparkLauncher
import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Matchers._
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{ExecuteRequest, JobHandle, LivyBaseUnitTestSuite, LivyConf}
import com.cloudera.livy.rsc.{PingJob, RSCClient, RSCConf}
import com.cloudera.livy.rsc.driver.StatementState
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions.{PySpark, SessionState, Spark}
import com.cloudera.livy.utils.{AppInfo, SparkApp}

class InteractiveSessionSpec extends FunSpec
    with Matchers with BeforeAndAfterAll with LivyBaseUnitTestSuite {

  private val livyConf = new LivyConf()
  livyConf.set(InteractiveSession.LIVY_REPL_JARS, "")
    .set(LivyConf.LIVY_SPARK_VERSION, "1.6.0")
    .set(LivyConf.LIVY_SPARK_SCALA_VERSION, "2.10.5")

  implicit val formats = DefaultFormats

  private var session: InteractiveSession = null

  private def createSession(
      sessionStore: SessionStore = mock[SessionStore],
      mockApp: Option[SparkApp] = None): InteractiveSession = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")

    val req = new CreateInteractiveRequest()
    req.kind = PySpark()
    req.driverMemory = Some("512m")
    req.driverCores = Some(1)
    req.executorMemory = Some("512m")
    req.executorCores = Some(1)
    req.name = Some("InteractiveSessionSpec")
    req.conf = Map(
      SparkLauncher.DRIVER_EXTRA_CLASSPATH -> sys.props("java.class.path"),
      RSCConf.Entry.LIVY_JARS.key() -> ""
    )
    InteractiveSession.create(0, null, None, livyConf, req, sessionStore, mockApp)
  }

  private def executeStatement(code: String): JValue = {
    val id = session.executeStatement(ExecuteRequest(code)).id
    eventually(timeout(30 seconds), interval(100 millis)) {
      val s = session.getStatement(id).get
      s.state shouldBe StatementState.Available
      parse(s.output)
    }
  }

  override def afterAll(): Unit = {
    if (session != null) {
      Await.ready(session.stop(), 30 seconds)
      session = null
    }
    super.afterAll()
  }

  private def withSession(desc: String)(fn: (InteractiveSession) => Unit): Unit = {
    it(desc) {
      assume(session != null, "No active session.")
      eventually(timeout(30 seconds), interval(100 millis)) {
        session.state shouldBe a[SessionState.Idle]
      }
      fn(session)
    }
  }

  describe("A spark session") {
    it("should start in the idle state") {
      session = createSession()
      session.state should (be(a[SessionState.Starting]) or be(a[SessionState.Idle]))
    }

    it("should update appId and appInfo and session store") {
      val mockApp = mock[SparkApp]
      val sessionStore = mock[SessionStore]
      val session = createSession(sessionStore, Some(mockApp))

      val expectedAppId = "APPID"
      session.appIdKnown(expectedAppId)
      session.appId shouldEqual Some(expectedAppId)

      val expectedAppInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
      session.infoChanged(expectedAppInfo)
      session.appInfo shouldEqual expectedAppInfo

      verify(sessionStore, atLeastOnce()).save(
        MockitoMatchers.eq(InteractiveSession.RECOVERY_SESSION_TYPE), anyObject())
    }

    withSession("should execute `1 + 2` == 3") { session =>
      val result = executeStatement("1 + 2")
      val expectedResult = Extraction.decompose(Map(
        "status" -> "ok",
        "execution_count" -> 0,
        "data" -> Map(
          "text/plain" -> "3"
        )
      ))

      result should equal (expectedResult)
    }

    withSession("should report an error if accessing an unknown variable") { session =>
      val result = executeStatement("x")
      val expectedResult = Extraction.decompose(Map(
        "status" -> "error",
        "execution_count" -> 1,
        "ename" -> "NameError",
        "evalue" -> "name 'x' is not defined",
        "traceback" -> List(
          "Traceback (most recent call last):\n",
          "NameError: name 'x' is not defined\n"
        )
      ))

      result should equal (expectedResult)
      session.state shouldBe a[SessionState.Idle]
    }

    withSession("should error out the session if the interpreter dies") { session =>
      executeStatement("import os; os._exit(666)")
      (session.state match {
        case SessionState.Error(_) => true
        case _ => false
      }) should equal(true)
    }
  }

  describe("recovery") {
    it("should recover session") {
      val conf = new LivyConf()
      val sessionStore = mock[SessionStore]
      val mockClient = mock[RSCClient]
      when(mockClient.submit(any(classOf[PingJob]))).thenReturn(mock[JobHandle[Void]])
      val m =
        InteractiveRecoveryMetadata(78, None, "appTag", Spark(), null, None, Some(URI.create("")))
      val s = InteractiveSession.recover(m, conf, sessionStore, None, Some(mockClient))

      s.state shouldBe a[SessionState.Recovering]

      s.appIdKnown("appId")
      verify(sessionStore, atLeastOnce()).save(
        MockitoMatchers.eq(InteractiveSession.RECOVERY_SESSION_TYPE), anyObject())
    }

    it("should recover session to dead state if rscDriverUri is unknown") {
      val conf = new LivyConf()
      val sessionStore = mock[SessionStore]
      val m = InteractiveRecoveryMetadata(78, Some("appId"), "appTag", Spark(), null, None, None)
      val s = InteractiveSession.recover(m, conf, sessionStore, None)

      s.state shouldBe a[SessionState.Dead]
      s.logLines().mkString should include("RSCDriver URI is unknown")
    }
  }
}
