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
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent._
import scala.concurrent.duration._

import org.json4s.jackson.Json4sScalaModule
import org.scalatra._
import org.scalatra.servlet.FileUploadSupport

import com.cloudera.livy.{ExecuteRequest, JobHandle, LivyConf, Logging}
import com.cloudera.livy.client.common.HttpMessages
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions._

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(livyConf: LivyConf)
  extends SessionServlet[InteractiveSession](livyConf)
  with FileUploadSupport
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

    new SessionInfo(session.id, null, session.owner, session.proxyUser.orNull,
      session.state.toString, session.kind.toString, logs.asJava)
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

  private def slice(from: Option[Int], to: Option[Int], size: Option[Int], offset: Option[Int],
                    items: TreeMap[Int, Statement]): TreeMap[Int, Statement] = {

    // Can't slice an empty list
    if (items.isEmpty) {
      return items
    }

    // No slicing
    if (from.isEmpty && to.isEmpty && size.isEmpty && offset.isEmpty) {
      return items
    }

    // Size only
    if (from.isEmpty && to.isEmpty && size.isDefined && offset.isEmpty) {
      return items.takeRight(size.get)
    }

    // Size + offset
    if (from.isEmpty && to.isEmpty && size.isDefined && offset.isDefined) {
      return items.takeRight(size.get + offset.get).take(size.get)
    }

    // From only
    if (from.isDefined && to.isEmpty && size.isEmpty && offset.isEmpty) {
      return items.from(from.get)
    }

    // From + size
    if (from.isDefined && to.isEmpty && size.isDefined && offset.isEmpty) {
      return items.from(from.get).take(size.get)
    }

    // To only
    if (from.isEmpty && to.isDefined && size.isEmpty && offset.isEmpty) {
      return items.to(to.get)
    }

    // To + size
    if (from.isEmpty && to.isDefined && size.isDefined && offset.isEmpty) {
      return items.to(to.get).takeRight(size.get)
    }

    // From + to
    if (from.isDefined && to.isDefined && size.isEmpty && offset.isEmpty) {
      return items.from(from.get).to(to.get)
    }

    throw new IllegalStateException()
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
      val from = params.get("from").map(_.toInt)
      val to = params.get("to").map(_.toInt)
      val size = params.get("size").map(_.toInt)
      val offset = params.get("offset").map(_.toInt)

      Map(
        "total_statements" -> session.statements.size,
        "statements" -> slice(from, to, size, offset, session.statements).mapValues(statementView)
      )
    }
  }

  val getStatement = get("/:id/statements/:statementId") {
    withSession { session =>
      val statementId = params("statementId").toInt
      val statement = session.statements.get(statementId)

      statement match {
        case None => NotFound("Statement not found")
        case Some(statement) =>
          statementView(statement)
      }
    }
  }

  delete("/:id/statements/:statementId") {
    withSession { session =>
      val statementId = params("statementId").toInt
      session.removeStatement(statementId)
      Ok()
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
