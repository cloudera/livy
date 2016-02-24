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

import java.io.File
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.{ArrayList, HashMap}
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletResponse._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.api.java.function.VoidFunction
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{Job, JobContext, JobHandle, LivyConf}
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.client.local.LocalConf
import com.cloudera.livy.server.{BaseSessionServletSpec, RemoteUserOverride}

class ClientServletSpec
  extends BaseSessionServletSpec[ClientSession] {

  private val PROXY = "__proxy__"

  override def createServlet(): ClientSessionServlet = {
    new ClientSessionServlet(createConf()) with RemoteUserOverride
  }

  private var sessionId: Int = -1

  def withSessionId(desc: String)(fn: (Int) => Unit): Unit = {
    it(desc) {
      assume(sessionId != -1, "No active session.")
      fn(sessionId)
    }
  }

  describe("Client Servlet") {

    it("should create client sessions") {
      jpost[SessionInfo]("/", createRequest()) { data =>
        header("Location") should equal("/0")
        data.id should equal (0)
        sessionId = data.id
      }
    }

    it("should list existing sessions") {
      jget[Map[String, Any]]("/") { data =>
        data("sessions") match {
          case contents: Seq[_] => contents.size should equal (1)
          case _ => fail("Response is not an array.")
        }
      }
    }

    withSessionId("should handle asynchronous jobs") { testJobSubmission(_, false) }

    withSessionId("should handle synchronous jobs") { testJobSubmission(_, true) }

    // Test that the file does get copied over to the live home dir on HDFS - does not test end
    // to end that the LocalClient class copies it over to the app.
    withSessionId("should support file uploads") { id =>
      testResourceUpload("file", id)
    }

    withSessionId("should support jar uploads") { id =>
      testResourceUpload("jar", id)
    }

    withSessionId("should monitor async Spark jobs") { sid =>
      val ser = new Serializer()
      val job = BufferUtils.toByteArray(ser.serialize(new AsyncTestJob()))
      var jobId: Long = -1L
      val collectedSparkJobs = new ArrayList[Integer]()
      jpost[JobStatus](s"/$sid/submit-job", new SerializedJob(job)) { status =>
        jobId = status.id
        if (status.newSparkJobs != null) {
          collectedSparkJobs.addAll(status.newSparkJobs)
        }
      }

      eventually(timeout(1 minute), interval(100 millis)) {
        jget[JobStatus](s"/$sid/jobs/$jobId") { status =>
          if (status.newSparkJobs != null) {
            collectedSparkJobs.addAll(status.newSparkJobs)
          }
          status.state should be (JobHandle.State.SUCCEEDED)
        }
      }

      collectedSparkJobs.size should be (1)
    }

    withSessionId("should tear down sessions") { id =>
      jdelete[Map[String, Any]](s"/$id") { data =>
        data should equal (Map("msg" -> "deleted"))
      }
      jget[Map[String, Any]]("/") { data =>
        data("sessions") match {
          case contents: Seq[_] => contents.size should equal (0)
          case _ => fail("Response is not an array.")
        }
      }
    }

    it("should support user impersonation") {
      val headers = makeUserHeaders(PROXY)
      jpost[SessionInfo]("/", createRequest(inProcess = false), headers = headers) { data =>
        try {
          data.owner should be (PROXY)
          data.proxyUser should be (PROXY)
          val user = runJob(data.id, new GetUserJob(), headers = headers)
          user should be (PROXY)
        } finally {
          deleteSession(data.id)
        }
      }
    }

    it("should honor impersonation requests") {
      val request = createRequest(inProcess = false)
      request.conf.put(LocalConf.Entry.PROXY_USER.key(), PROXY)
      jpost[SessionInfo]("/", request, headers = adminHeaders) { data =>
        try {
          data.owner should be (ADMIN)
          data.proxyUser should be (PROXY)
          val user = runJob(data.id, new GetUserJob(), headers = adminHeaders)
          user should be (PROXY)
        } finally {
          deleteSession(data.id)
        }
      }
    }
  }

  private def deleteSession(id: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = adminHeaders) { _ => }
  }

  private def createRequest(inProcess: Boolean = true): CreateClientRequest = {
    val classpath = sys.props("java.class.path")
    val conf = new HashMap[String, String]
    conf.put("spark.master", "local")
    conf.put("livy.local.jars", "")
    conf.put("spark.driver.extraClassPath", classpath)
    conf.put("spark.executor.extraClassPath", classpath)
    if (inProcess) {
      conf.put(LocalConf.Entry.CLIENT_IN_PROCESS.key(), "true")
    }
    new CreateClientRequest(10000L, conf)
  }

  private def testResourceUpload(cmd: String, sessionId: Int): Unit = {
    val f = File.createTempFile("uploadTestFile", cmd)
    val conf = new LivyConf()

    Files.write(Paths.get(f.getAbsolutePath), "Test data".getBytes())

    jupload[Unit](s"/$sessionId/upload-$cmd", Map(cmd -> f), expectedStatus = SC_OK) { _ =>
      val resultFile = new File(new URI(s"${conf.livyHome()}/$sessionId/${f.getName}"))
      resultFile.deleteOnExit()
      resultFile.exists() should be(true)
      Source.fromFile(resultFile).mkString should be("Test data")
    }
  }

  private def testJobSubmission(sid: Int, sync: Boolean): Unit = {
    val result = runJob(sid, new TestJob(), sync = sync)
    result should be (42)
  }

  private def runJob[T](
      sid: Int,
      job: Job[T],
      sync: Boolean = false,
      headers: Map[String, String] = defaultHeaders): T = {
    val ser = new Serializer()
    val jobData = BufferUtils.toByteArray(ser.serialize(job))
    val route = if (sync) s"/$sid/submit-job" else s"/$sid/run-job"
    var jobId: Long = -1L
    jpost[JobStatus](route, new SerializedJob(jobData), headers = headers) { data =>
      jobId = data.id
    }

    var result: Option[T] = None
    eventually(timeout(1 minute), interval(100 millis)) {
      jget[JobStatus](s"/$sid/jobs/$jobId") { status =>
        status.id should be (jobId)
        status.state should be (JobHandle.State.SUCCEEDED)
        result = Some(ser.deserialize(ByteBuffer.wrap(status.result)).asInstanceOf[T])
      }
    }
    result.getOrElse(throw new IllegalStateException())
  }

}

class TestJob extends Job[Int] {

  override def call(jc: JobContext): Int = 42

}

class AsyncTestJob extends Job[Int] {

  override def call(jc: JobContext): Int = {
    val future = jc.sc().parallelize(List[Integer](1, 2, 3).asJava).foreachAsync(
      new VoidFunction[Integer]() {
        override def call(l: Integer): Unit = Thread.sleep(1)
      })
    jc.monitor(future)
    future.get(10, TimeUnit.SECONDS)
    42
  }

}

class GetUserJob extends Job[String] {

  override def call(jc: JobContext): String = UserGroupInformation.getCurrentUser().getUserName()

}
