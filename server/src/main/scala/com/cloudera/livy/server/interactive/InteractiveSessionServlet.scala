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

import com.cloudera.livy.{ExecuteRequest, Logging}
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.interactive.{InteractiveSession, Statement}
import com.cloudera.livy.spark.interactive.CreateInteractiveRequest
import org.json4s.jackson.Json4sScalaModule
import org.scalatra._

import scala.concurrent._
import scala.concurrent.duration._

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(
    sessionManager: SessionManager[InteractiveSession, CreateInteractiveRequest])
  extends SessionServlet(sessionManager)
{

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  override protected def clientSessionView(session: InteractiveSession): Any = {
    val lines = session.logLines()

    val size = 10
    var from = math.max(0, lines.length - size)
    val until = from + size

    val logs = lines.view(from, until)

    Map(
      "id" -> session.id,
      "state" -> session.state.toString,
      "kind" -> session.kind.toString,
      "proxyUser" -> session.proxyUser,
      "logs" -> logs)
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

  jpost[CallbackRequest]("/:sessionId/callback") { callback =>
    val sessionId = params("sessionId").toInt

    sessionManager.get(sessionId) match {
      case Some(session) =>
        if (session.state == SessionState.Starting()) {
          session.url = new URL(callback.url)
          Accepted()
        } else if (session.state.isActive) {
          Ok()
        } else {
          BadRequest("Session is in wrong state")
        }
      case None => NotFound("Session not found")
    }
  }

  post("/:sessionId/stop") {
    val sessionId = params("sessionId").toInt
    sessionManager.get(sessionId) match {
      case Some(session) =>
        val future = session.stop()

        new AsyncResult() { val is = for { _ <- future } yield NoContent() }
      case None => NotFound("Session not found")
    }
  }

  post("/:sessionId/interrupt") {
    val sessionId = params("sessionId").toInt
    sessionManager.get(sessionId) match {
      case Some(session) =>
        val future = for {
          _ <- session.interrupt()
        } yield Ok(Map("msg" -> "interrupted"))

        // FIXME: this is silently eating exceptions.
        new AsyncResult() { val is = future }
      case None => NotFound("Session not found")
    }
  }

  get("/:sessionId/statements") {
    val sessionId = params("sessionId").toInt

    sessionManager.get(sessionId) match {
      case None => NotFound("Session not found")
      case Some(session: InteractiveSession) =>
        val from = params.get("from").map(_.toInt).getOrElse(0)
        val size = params.get("size").map(_.toInt).getOrElse(session.statements.length)

        Map(
          "total_statements" -> session.statements.length,
          "statements" -> session.statements.view(from, from + size).map(statementView)
        )
    }
  }

  val getStatement = get("/:sessionId/statements/:statementId") {
    val sessionId = params("sessionId").toInt
    val statementId = params("statementId").toInt

    val from = params.get("from").map(_.toInt)
    val size = params.get("size").map(_.toInt)

    sessionManager.get(sessionId) match {
      case None => NotFound("Session not found")
      case Some(session) =>
        session.statements.lift(statementId) match {
          case None => NotFound("Statement not found")
          case Some(statement) =>
            statementView(statement)
        }
    }
  }

  jpost[ExecuteRequest]("/:sessionId/statements") { req =>
    val sessionId = params("sessionId").toInt

    sessionManager.get(sessionId) match {
      case Some(session) =>
        val statement = session.executeStatement(req)

        Created(statementView(statement),
          headers = Map(
            "Location" -> url(getStatement,
              "sessionId" -> session.id.toString,
              "statementId" -> statement.id.toString)))
      case None => NotFound("Session not found")
    }
  }
}

private case class CallbackRequest(url: String)
