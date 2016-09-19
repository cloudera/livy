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

import java.util.concurrent.atomic.AtomicInteger
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.json4s.JsonAST._
import org.json4s.jackson.Json4sScalaModule
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Entry
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.ExecuteRequest
import com.cloudera.livy.client.common.HttpMessages.SessionInfo
import com.cloudera.livy.sessions._
import com.cloudera.livy.utils.AppInfo

class InteractiveSessionServletSpec extends BaseInteractiveServletSpec {

  mapper.registerModule(new Json4sScalaModule())

  class MockInteractiveSessionServlet extends InteractiveSessionServlet(createConf()) {

    private var statements = IndexedSeq[Statement]()

    override protected def createSession(req: HttpServletRequest): InteractiveSession = {
      val statementCounter = new AtomicInteger()

      val session = mock[InteractiveSession]
      when(session.kind).thenReturn(Spark())
      when(session.appId).thenReturn(None)
      when(session.appInfo).thenReturn(AppInfo())
      when(session.logLines()).thenReturn(IndexedSeq())
      when(session.state).thenReturn(SessionState.Idle())
      when(session.stop()).thenReturn(Future.successful(()))
      when(session.proxyUser).thenReturn(None)
      when(session.statements).thenAnswer(
        new Answer[IndexedSeq[Statement]]() {
          override def answer(args: InvocationOnMock): IndexedSeq[Statement] = statements
        })
      when(session.executeStatement(any(classOf[ExecuteRequest]))).thenAnswer(
        new Answer[Statement]() {
          override def answer(args: InvocationOnMock): Statement = {
            val id = statementCounter.getAndIncrement
            val executeRequest = args.getArguments()(0).asInstanceOf[ExecuteRequest]
            val statement = new Statement(
              id,
              executeRequest,
              Future.successful(JObject(JField("value", JInt(42)))))

            statements :+= statement
            statement
          }
        })

      session
    }

  }

  override def createServlet(): InteractiveSessionServlet = new MockInteractiveSessionServlet()

  it("should setup and tear down an interactive session") {
    jget[Map[String, Any]]("/") { data =>
      data("sessions") should equal(Seq())
    }

    jpost[Map[String, Any]]("/", createRequest()) { data =>
      header("Location") should equal("/0")
      data("id") should equal (0)

      val session = servlet.sessionManager.get(0)
      session should be (defined)
    }

    jget[Map[String, Any]]("/0") { data =>
      data("id") should equal (0)
      data("state") should equal ("idle")

      val batch = servlet.sessionManager.get(0)
      batch should be (defined)
    }

    jpost[Map[String, Any]]("/0/statements", ExecuteRequest("foo")) { data =>
      data("id") should be (0)
      data("output") should be (Map("value" -> 42))
    }

    jget[Map[String, Any]]("/0/statements") { data =>
      data("total_statements") should be (1)
      data("statements").asInstanceOf[Seq[Map[String, Any]]](0)("id") should be (0)
    }

    jdelete[Map[String, Any]]("/0") { data =>
      data should equal (Map("msg" -> "deleted"))

      val session = servlet.sessionManager.get(0)
      session should not be defined
    }
  }

  it("should show session properties") {
    val id = 0
    val appId = "appid"
    val owner = "owner"
    val proxyUser = "proxyUser"
    val state = SessionState.Running()
    val kind = Spark()
    val appInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
    val log = IndexedSeq[String]("log1", "log2")

    val session = mock[InteractiveSession]
    when(session.id).thenReturn(id)
    when(session.appId).thenReturn(Some(appId))
    when(session.owner).thenReturn(owner)
    when(session.proxyUser).thenReturn(Some(proxyUser))
    when(session.state).thenReturn(state)
    when(session.kind).thenReturn(kind)
    when(session.appInfo).thenReturn(appInfo)
    when(session.logLines()).thenReturn(log)

    val req = mock[HttpServletRequest]

    val view = servlet.asInstanceOf[InteractiveSessionServlet].clientSessionView(session, req)
      .asInstanceOf[SessionInfo]

    view.id shouldEqual id
    view.appId shouldEqual appId
    view.owner shouldEqual owner
    view.proxyUser shouldEqual proxyUser
    view.state shouldEqual state.toString
    view.kind shouldEqual kind.toString
    view.appInfo should contain (Entry(AppInfo.DRIVER_LOG_URL_NAME, appInfo.driverLogUrl.get))
    view.appInfo should contain (Entry(AppInfo.SPARK_UI_URL_NAME, appInfo.sparkUiUrl.get))
    view.log shouldEqual log.asJava
  }

}
