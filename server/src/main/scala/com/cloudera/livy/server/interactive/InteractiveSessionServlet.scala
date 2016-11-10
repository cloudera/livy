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
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._

import org.json4s.jackson.Json4sScalaModule
import org.scalatra._
import org.scalatra.servlet.FileUploadSupport

import com.cloudera.livy.{ExecuteRequest, JobHandle, LivyConf, Logging}
import com.cloudera.livy.client.common.HttpMessages
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.rsc.driver.Statement
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions._

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(
    sessionManager: InteractiveSessionManager,
    sessionStore: SessionStore,
    livyConf: LivyConf)
  extends SessionServlet(sessionManager, livyConf)
  with FileUploadSupport
{

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  override protected def createSession(req: HttpServletRequest): InteractiveSession = {
    val createRequest = bodyAs[CreateInteractiveRequest](req)
    val proxyUser = checkImpersonation(createRequest.proxyUser, req)
    InteractiveSession.create(
      sessionManager.nextId(),
      remoteUser(req),
      proxyUser,
      livyConf,
      createRequest,
      sessionStore)
  }

  override protected[interactive] def clientSessionView(
      session: InteractiveSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (hasAccess(session.owner, req)) {
        Option(session.logLines())
          .map { lines =>
            val size = 10
            var from = math.max(0, lines.length - size)
            val until = from + size

            lines.view(from, until)
          }
          .getOrElse(Nil)
      } else {
        Nil
      }

    new SessionInfo(session.id, session.appId.orNull, session.owner, session.proxyUser.orNull,
      session.state.toString, session.kind.toString, session.appInfo.asJavaMap, logs.asJava)
  }

  post("/:id/stop") {
    withSession { session =>
      Await.ready(session.stop(), Duration.Inf)
      NoContent()
    }
  }

  post("/:id/interrupt") {
    withSession { session =>
      Await.ready(session.interrupt(), Duration.Inf)
      Ok(Map("msg" -> "interrupted"))
    }
  }

  get("/:id/statements") {
    withSession { session =>
      val statements = session.statements
      val from = params.get("from").map(_.toInt).getOrElse(0)
      val size = params.get("size").map(_.toInt).getOrElse(statements.length)

      Map(
        "total_statements" -> statements.length,
        "statements" -> statements.view(from, from + size)
      )
    }
  }

  val getStatement = get("/:id/statements/:statementId") {
    withSession { session =>
      val statementId = params("statementId").toInt

      session.getStatement(statementId).getOrElse(NotFound("Statement not found"))
    }
  }

  jpost[ExecuteRequest]("/:id/statements") { req =>
    withSession { session =>
      val statement = session.executeStatement(req)

      Created(statement,
        headers = Map(
          "Location" -> url(getStatement,
            "id" -> session.id.toString,
            "statementId" -> statement.id.toString)))
    }
  }

  // This endpoint is used by the client-http module to "connect" to an existing session and
  // update its last activity time. It performs authorization checks to make sure the caller
  // has access to the session, so even though it returns the same data, it behaves differently
  // from get("/:id").
  post("/:id/connect") {
    withSession { session =>
      session.recordActivity()
      Ok(clientSessionView(session, request))
    }
  }

  jpost[SerializedJob]("/:id/submit-job") { req =>
    withSession { session =>
      try {
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.submitJob(req.job)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
      } catch {
        case e: Throwable =>
          e.printStackTrace()
        throw e
      }
    }
  }

  jpost[SerializedJob]("/:id/run-job") { req =>
    withSession { session =>
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.runJob(req.job)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
    }
  }

  post("/:id/upload-jar") {
    withSession { lsession =>
      fileParams.get("jar") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No jar sent!")
      }
    }
  }

  post("/:id/upload-pyfile") {
    withSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  post("/:id/upload-file") {
    withSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  jpost[AddResource]("/:id/add-jar") { req =>
    withSession { lsession =>
      addJarOrPyFile(req, lsession)
    }
  }

  jpost[AddResource]("/:id/add-pyfile") { req =>
    withSession { lsession =>
      lsession.kind match {
        case PySpark() | PySpark3() => addJarOrPyFile(req, lsession)
        case _ => BadRequest("Only supported for pyspark sessions.")
      }
    }
  }

  jpost[AddResource]("/:id/add-file") { req =>
    withSession { lsession =>
      val uri = new URI(req.uri)
      lsession.addFile(uri)
    }
  }

  get("/:id/jobs/:jobid") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      Ok(lsession.jobStatus(jobId))
    }
  }

  post("/:id/jobs/:jobid/cancel") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      lsession.cancelJob(jobId)
    }
  }

  private def addJarOrPyFile(req: HttpMessages.AddResource, session: InteractiveSession): Unit = {
    val uri = new URI(req.uri)
    session.addJar(uri)
  }
}
