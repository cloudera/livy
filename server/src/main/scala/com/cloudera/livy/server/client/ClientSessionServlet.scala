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

package com.cloudera.livy.server.client

import java.net.URI
import javax.servlet.http.HttpServletRequest

import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig}

import com.cloudera.livy.{JobHandle, LivyConf}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.client.local.LocalConf
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.SessionManager

class ClientSessionServlet(livyConf: LivyConf)
  extends SessionServlet[ClientSession](livyConf)
  with FileUploadSupport {

  configureMultipartHandling(MultipartConfig(maxFileSize =
    Some(livyConf.getLong(LivyConf.FILE_UPLOAD_MAX_SIZE))))

  override protected def createSession(req: HttpServletRequest): ClientSession = {
    val id = sessionManager.nextId()
    val createRequest = bodyAs[CreateClientRequest](req)
    val user = remoteUser(req)
    val requestedProxy =
      if (createRequest.conf != null) {
        Option(createRequest.conf.get(LocalConf.Entry.PROXY_USER.key()))
      } else {
        None
      }
    val proxyUser = checkImpersonation(requestedProxy, req)
    new ClientSession(id, user, proxyUser, createRequest, livyConf.livyHome)
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
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null, null))
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
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null, null))
    }
  }

  post("/:id/upload-jar") {
    withSession { lsession =>
      fileParams.get("jar") match {
        case Some(file) =>
          doAsync {
            lsession.addJar(file.getInputStream, file.name)
          }
        case None =>
          BadRequest("No jar uploaded!")
      }
    }
  }

  post("/:id/upload-file") {
    withSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          doAsync {
            lsession.addFile(file.getInputStream, file.name)
          }
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  jpost[AddResource]("/:id/add-jar") { req =>
    withSession { lsession =>
      val uri = new URI(req.uri)
      doAsync {
        lsession.addJar(uri)
      }
    }
  }

  jpost[AddResource]("/:id/add-file") { req =>
    withSession { lsession =>
      val uri = new URI(req.uri)
      doAsync {
        lsession.addFile(uri)
      }
    }
  }

  get("/:id/jobs/:jobid") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      doAsync { Ok(lsession.jobStatus(jobId)) }
    }
  }

  post("/:id/jobs/:jobid/cancel") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      doAsync { lsession.cancelJob(jobId) }
    }
  }

  override protected def clientSessionView(session: ClientSession, req: HttpServletRequest): Any = {
    new SessionInfo(session.id, session.owner, session.proxyUser.getOrElse(null),
      session.state.toString)
  }

}
