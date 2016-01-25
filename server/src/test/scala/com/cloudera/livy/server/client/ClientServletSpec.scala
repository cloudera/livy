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

import java.nio.ByteBuffer
import java.util.{HashMap, Map => JMap}

import scala.concurrent.duration._
import scala.language.postfixOps

import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{Job, JobContext}
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.BaseSessionServletSpec
import com.cloudera.livy.spark.client._

class ClientServletSpec extends BaseSessionServletSpec[ClientSession] {

  override def sessionFactory = new ClientSessionFactory()
  override def servlet = new ClientSessionServlet(sessionManager)
  override protected implicit lazy val jsonFormats: Formats = DefaultFormats ++ Serializers.Formats

  private val mapper = new ObjectMapper()
  private var sessionId: Int = -1

  def withSessionId(desc: String)(fn: (Int) => Unit): Unit = {
    it(desc) {
      assume(sessionId != -1, "No active session.")
      fn(sessionId)
    }
  }

  describe("Client Servlet") {

    it("should create client sessions") {
      val classpath = sys.props("java.class.path")
      val conf = new HashMap[String, String]
      conf.put("spark.master", "local")
      conf.put("livy.local.jars", "")
      conf.put("spark.driver.extraClassPath", classpath)
      conf.put("spark.executor.extraClassPath", classpath)

      postJson("/", new CreateClientRequest(10000L, conf)) { data =>
        header("Location") should equal("/0")
        data \ "id" should equal (JInt(0))
        sessionId = (data \ "id").extract[Int]
      }
    }

    it("should list existing sessions") {
      getJson("/") { data =>
        (data \ "sessions") match {
          case JArray(contents) => contents.size should equal (1)
          case _ => fail("Response is not an array.")
        }
      }
    }

    withSessionId("should handle asynchronous jobs") { testJobSubmission(_, false) }

    withSessionId("should handle synchronous jobs") { testJobSubmission(_, true) }

    withSessionId("should tear down sessions") { id =>
      deleteJson(s"/$id") { data =>
        // TODO: check data when it exists.
      }
      getJson("/") { data =>
        (data \ "sessions") match {
          case JArray(contents) => contents.size should equal (0)
          case _ => fail("Response is not an array.")
        }
      }
    }

  }

  // Because json4s does not know how to serialize obscure types such as JAVA MAPS AND ENUMS, we
  // override the base class so that we use a sane library for serialization.
  override protected def toJson(msg: AnyRef): String = mapper.writeValueAsString(msg)

  private def testJobSubmission(sid: Int, sync: Boolean): Unit = {
    val ser = new Serializer()
    val job = BufferUtils.toByteArray(ser.serialize(new TestJob()))
    val route = if (sync) s"/$sid/submit-job" else s"/$sid/run-job"
    var jobId: Long = -1L
    postJson(route, new SerializedJob(job)) { data =>
      jobId = data.extract[JobStatus].id
    }
    eventually(timeout(1 minute), interval(100 millis)) {
      getJson(s"/$sid/jobs/$jobId") { statusData =>
        val status = statusData.extract[JobStatus]
        status.id should be (jobId)
        val result = ser.deserialize(ByteBuffer.wrap(status.result))
        result should be (42)
      }
    }
  }

}

class TestJob extends Job[Int] {

  override def call(jc: JobContext): Int = 42

}
