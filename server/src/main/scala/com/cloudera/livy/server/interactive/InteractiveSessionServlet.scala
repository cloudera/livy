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

import java.net.URL
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.concurrent._
import scala.concurrent.duration._

import org.json4s.jackson.Json4sScalaModule
import org.scalatra._

import com.cloudera.livy.{ExecuteRequest, LivyConf, Logging}
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions._

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(livyConf: LivyConf)
  extends SessionServlet[InteractiveSession](livyConf)
{

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  override protected def createSession(req: HttpServletRequest): InteractiveSession = {
    val createRequest = bodyAs[CreateInteractiveRequest](req)
    val proxyUser = checkImpersonation(createRequest.proxyUser, req)
    new InteractiveSession(sessionManager.nextId(), remoteUser(req), proxyUser, livyConf,
      createRequest)
  }

  override protected def clientSessionView(
      session: InteractiveSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (hasAccess(session.owner, req)) {
        val lines = session.logLines()

        val size = 10
        var from = math.max(0, lines.length - size)
        val until = from + size

        lines.view(from, until)
      } else {
        Nil
      }

    Map(
      "id" -> session.id,
      "state" -> session.state.toString,
      "kind" -> session.kind.toString,
      "proxyUser" -> session.proxyUser,
      "log" -> logs)
  }

  private def statementView(statement: Statement): Any = {
    val output = try {
      Await.result(statement.output(), Duration(100, TimeUnit.MILLISECONDS))
    } catch {
      case _: TimeoutException => null
    }
    Map(
      "id" -> statement.id,
      "state" -> statement.state.toString,
      "output" -> output)
  }

  jpost[CallbackRequest]("/:id/callback") { callback =>
    withUnprotectedSession { session =>
      if (session.state == SessionState.Starting()) {
        session.url = new URL(callback.url)
        Accepted()
      } else if (session.state.isActive) {
        Ok()
      } else {
        BadRequest("Session is in wrong state")
      }
    }
  }

  post("/:id/stop") {
    withSession { session =>
      val future = session.stop()
      new AsyncResult() { val is = for { _ <- future } yield NoContent() }
    }
  }

  post("/:id/interrupt") {
    withSession { session =>
      val future = for {
        _ <- session.interrupt()
      } yield Ok(Map("msg" -> "interrupted"))

      // FIXME: this is silently eating exceptions.
      new AsyncResult() { val is = future }
    }
  }

  get("/:id/statements") {
    withSession { session =>
      val from = params.get("from").map(_.toInt).getOrElse(0)
      val size = params.get("size").map(_.toInt).getOrElse(session.statements.length)

      Map(
        "total_statements" -> session.statements.length,
        "statements" -> session.statements.view(from, from + size).map(statementView)
      )
    }
  }

  val getStatement = get("/:id/statements/:statementId") {
    withSession { session =>
      val statementId = params("statementId").toInt
      val from = params.get("from").map(_.toInt)
      val size = params.get("size").map(_.toInt)

      session.statements.lift(statementId) match {
        case None => NotFound("Statement not found")
        case Some(statement) =>
          statementView(statement)
      }
    }
  }

  jpost[ExecuteRequest]("/:id/statements") { req =>
    withSession { session =>
      val statement = session.executeStatement(req)

      Created(statementView(statement),
        headers = Map(
          "Location" -> url(getStatement,
            "id" -> session.id.toString,
            "statementId" -> statement.id.toString)))
    }
  }

}

private case class CallbackRequest(url: String)
