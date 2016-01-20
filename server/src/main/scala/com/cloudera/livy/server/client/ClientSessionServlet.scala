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

import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.scalatra._

import com.cloudera.livy.JobHandle
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.{SessionManager, SessionState}
import com.cloudera.livy.spark.client._

class ClientSessionServlet(sessionManager: SessionManager[ClientSession])
  extends SessionServlet[ClientSession](sessionManager) {

  override protected implicit lazy val jsonFormats: Formats = DefaultFormats ++ Serializers.Formats

  post("/:id/submit-job") {
    withSession { session =>
      try {
      val req = extract[SerializedJob]
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

  post("/:id/run-job") {
    withSession { session =>
      val req = extract[SerializedJob]
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.runJob(req.job)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
    }
  }

  post("/:id/add-jar") {
    withSession { lsession =>
      val uri = new URI(extract[AddResource].uri)
      doAsync { lsession.addJar(uri) }
    }
  }

  post("/:id/add-file") {
    withSession { lsession =>
      val uri = new URI(extract[AddResource].uri)
      doAsync { lsession.addFile(uri) }
    }
  }

  get("/:id/jobs/:jobid") {
    withSession { lsession =>
      val jobId = params("jobid").toLong
      doAsync { Ok(lsession.jobStatus(jobId)) }
    }
  }

  private def extract[T: ClassTag](implicit tag: ClassTag[T]): T = {
    mapper.readValue(request.body, tag.runtimeClass).asInstanceOf[T]
  }

  override protected def serializeSession(session: ClientSession): JValue = {
    val view = new SessionInfo(session.id, session.state.toString)
    parse(write(view))
  }

}

/**
 * Serializers for the Java messages used for the client protocol. json4s seems to be miffed
 * by the existence of types that can be written in Java, and just refuses to serialize them,
 * or maybe it's Scalatra that is messing things up, but long story short, without this, the
 * Java messages are not serialized. Le sigh.
 */
private[client] object Serializers extends JsonMethods {

  val Formats: List[CustomSerializer[_]] = List(ClientMessageSerializer, StateSerializer)

  private implicit val jsonFormats: Formats = DefaultFormats ++ List(StateSerializer)

  private def serialize(msg: ClientMessage): JValue = {
    // This is horribly expensive but json4s makes it pretty much impossible to do anything
    // else. What a horrible library.
    parse(write(msg))
  }

  private case object ClientMessageSerializer extends CustomSerializer[ClientMessage](
    implicit formats => ( {
      // We don't need deserialization?
      PartialFunction.empty
    }, {
      case msg: ClientMessage => serialize(msg)
    })
  )

  private case object StateSerializer extends CustomSerializer[JobHandle.State](
    implicit formats => ( {
      case JString(value) => JobHandle.State.valueOf(value)
    }, {
      case state: JobHandle.State => JString(state.name())
    })
  )

}
