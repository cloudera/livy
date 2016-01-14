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
    sessionManager.get(params("id").toInt) match {
      case Some(session) =>
        val req = parsedBody.extract[SerializedJob]
        if (req.job != null && req.job.length > 0) {
          Created(session.submitJob(req.job))
        } else {
          BadRequest("No job provided.")
        }
      case None =>
    }
  }

  post("/:id/run-job") {
    sessionManager.get(params("id").toInt) match {
      case Some(session) =>
        val req = parsedBody.extract[SerializedJob]
        if (req.job != null && req.job.length > 0) {
          val jobId = session.runJob(req.job)
          Created(JobSubmitted(jobId))
        } else {
          BadRequest("No job provided.")
        }
      case None =>
    }
  }

  post("/:id/add-jar") {
    sessionManager.get(params("id").toInt) match {
      case Some(session) =>
        session.addJar(parsedBody.extract[AddJar].uri)
      case None =>
    }
  }

  post("/:id/add-file") {
    sessionManager.get(params("id").toInt) match {
      case Some(session) =>
        session.addFile(parsedBody.extract[AddFile].uri)
      case None =>
    }
  }

  post("/:id/job-status") {
    sessionManager.get(params("id").toInt) match {
      case Some(session) =>
      case None =>
    }
  }

  override protected def serializeSession(session: ClientSession): JValue = {
    val view = ClientSessionView(session.id, session.state)
    parse(write(view))
  }

  case class ClientSessionView(id: Int, state: SessionState)

}
