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

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.launcher.SparkLauncher
import org.json4s.{DefaultFormats, Extraction}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{ExecuteRequest, LivyConf}
import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.sessions.{PySpark, SessionState}

class InteractiveSessionSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  private val livyConf = new LivyConf()
  livyConf.set(InteractiveSession.LivyReplJars, "")

  implicit val formats = DefaultFormats

  private var session: InteractiveSession = null

  private def createSession(): InteractiveSession = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")

    val req = new CreateInteractiveRequest()
    req.kind = PySpark()
    req.conf = Map(
      SparkLauncher.DRIVER_EXTRA_CLASSPATH -> sys.props("java.class.path"),
      RSCConf.Entry.LIVY_JARS.key() -> ""
    )
    new InteractiveSession(0, null, None, livyConf, req)
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
        session.state should be (SessionState.Idle())
      }
      fn(session)
    }
  }

  describe("A spark session") {
    it("should start in the idle state") {
      session = createSession()
      session.state should (equal (SessionState.Starting()) or equal (SessionState.Idle()))
    }

    withSession("should execute `1 + 2` == 3") { session =>
      val stmt = session.executeStatement(ExecuteRequest("1 + 2"))
      val result = Await.result(stmt.output(), 30 seconds)

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
      val stmt = session.executeStatement(ExecuteRequest("x"))
      val result = Await.result(stmt.output(), 30 seconds)
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
      session.state should equal (SessionState.Idle())
    }

    withSession("should error out the session if the interpreter dies") { session =>
      val stmt = session.executeStatement(ExecuteRequest("import os; os._exit(1)"))
      val result = Await.result(stmt.output(), 30 seconds)
      (session.state match {
        case SessionState.Error(_) => true
        case _ => false
      }) should equal (true)
    }
  }

}
