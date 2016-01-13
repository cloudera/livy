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

import java.io.{ByteArrayInputStream, ObjectInputStream}

import scala.concurrent.Future

import org.json4s.JValue
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.scalatra._

import com.cloudera.livy.Job
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.{SessionManager, SessionState}
import com.cloudera.livy.spark.client._

class ClientSessionServlet(sessionManager: SessionManager[ClientSession])
  extends SessionServlet[ClientSession](sessionManager) {

  post("/:id/submit-job") {
    withSession { session =>
      val req = parsedBody.extract[SerializedJob]
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.submitJob(req.job)
      Created(JobSubmitted(jobId))
    }
  }

  post("/:id/run-job") {
    withSession { session =>
      val req = parsedBody.extract[SerializedJob]
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.runJob(req.job)
      Created(JobSubmitted(jobId))
    }
  }

  post("/:id/add-jar") {
    withSession { lsession =>
      val uri = parsedBody.extract[AddResource].uri
      doAsync { lsession.addJar(uri) }
    }
  }

  post("/:id/add-file") {
    withSession { lsession =>
      val uri = parsedBody.extract[AddResource].uri
      doAsync { lsession.addFile(uri) }
    }
  }

  get("/:id/jobs/:jobid") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      doAsync { Ok(lsession.jobStatus(jobId)) }
    }
  }

  override protected def serializeSession(session: ClientSession): JValue = {
    val view = ClientSessionView(session.id, session.state)
    parse(write(view))
  }

  case class ClientSessionView(id: Int, state: SessionState)

}
