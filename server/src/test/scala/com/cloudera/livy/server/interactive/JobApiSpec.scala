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

import java.io.File
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletResponse._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.api.java.function.VoidFunction
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{Job, JobContext, JobHandle}
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.server.RemoteUserOverride
import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.jobs.{Echo, GetCurrentUser}

class JobApiSpec extends BaseInteractiveServletSpec {

  private val PROXY = "__proxy__"

  private var sessionId: Int = -1

  override def createServlet(): InteractiveSessionServlet = {
    new InteractiveSessionServlet(createConf()) with RemoteUserOverride
  }

  def withSessionId(desc: String)(fn: (Int) => Unit): Unit = {
    it(desc) {
      assume(sessionId != -1, "No active session.")
      fn(sessionId)
    }
  }

  describe("Interactive Servlet") {

    it("should create sessions") {
      jpost[SessionInfo]("/", createRequest()) { data =>
        waitForIdle(data.id)
        header("Location") should equal("/0")
        data.id should equal (0)
        sessionId = data.id
      }
    }

    withSessionId("should handle asynchronous jobs") { testJobSubmission(_, false) }

    withSessionId("should handle synchronous jobs") { testJobSubmission(_, true) }

    // Test that the file does get copied over to the live home dir on HDFS - does not test end
    // to end that the RSCClient class copies it over to the app.
    withSessionId("should support file uploads") { id =>
      testResourceUpload("file", id)
    }

    withSessionId("should support jar uploads") { id =>
      testResourceUpload("jar", id)
    }

    withSessionId("should monitor async Spark jobs") { sid =>
      val ser = new Serializer()
      val job = BufferUtils.toByteArray(ser.serialize(new Echo("hello")))
      var jobId: Long = -1L
      jpost[JobStatus](s"/$sid/submit-job", new SerializedJob(job)) { status =>
        jobId = status.id
      }

      eventually(timeout(1 minute), interval(100 millis)) {
        jget[JobStatus](s"/$sid/jobs/$jobId") { status =>
          status.state should be (JobHandle.State.SUCCEEDED)
        }
      }
    }

    withSessionId("should update last activity on connect") { sid =>
      val currentActivity = servlet.sessionManager.get(sid).get.lastActivity
      jpost[SessionInfo](s"/$sid/connect", null, expectedStatus = SC_OK) { info =>
        val newActivity = servlet.sessionManager.get(sid).get.lastActivity
        assert(newActivity > currentActivity)
      }
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

      // Make sure the session's staging directory was cleaned up.
      assert(tempDir.listFiles().length === 0)
    }

    it("should support user impersonation") {
      val headers = makeUserHeaders(PROXY)
      jpost[SessionInfo]("/", createRequest(inProcess = false), headers = headers) { data =>
        try {
          waitForIdle(data.id)
          data.owner should be (PROXY)
          data.proxyUser should be (PROXY)
          val user = runJob(data.id, new GetCurrentUser(), headers = headers)
          user should be (PROXY)
        } finally {
          deleteSession(data.id)
        }
      }
    }

    it("should honor impersonation requests") {
      val request = createRequest(inProcess = false)
      request.proxyUser = Some(PROXY)
      jpost[SessionInfo]("/", request, headers = adminHeaders) { data =>
        try {
          waitForIdle(data.id)
          data.owner should be (ADMIN)
          data.proxyUser should be (PROXY)
          val user = runJob(data.id, new GetCurrentUser(), headers = adminHeaders)
          user should be (PROXY)

          // Test that files are uploaded to a new session directory.
          assert(tempDir.listFiles().length === 0)
          testResourceUpload("file", data.id)
        } finally {
          deleteSession(data.id)
          assert(tempDir.listFiles().length === 0)
        }
      }
    }

    it("should respect config black list") {
      jpost[SessionInfo]("/", createRequest(extraConf = BLACKLISTED_CONFIG),
        expectedStatus = SC_BAD_REQUEST) { _ => }
    }

  }

  private def waitForIdle(id: Int): Unit = {
    eventually(timeout(1 minute), interval(100 millis)) {
      jget[SessionInfo](s"/$id") { status =>
        status.state should be (SessionState.Idle().toString())
      }
    }
  }

  private def deleteSession(id: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = adminHeaders) { _ => }
  }

  private def testResourceUpload(cmd: String, sessionId: Int): Unit = {
    val f = File.createTempFile("uploadTestFile", cmd)
    val conf = createConf()

    Files.write(Paths.get(f.getAbsolutePath), "Test data".getBytes())

    jupload[Unit](s"/$sessionId/upload-$cmd", Map(cmd -> f), expectedStatus = SC_OK) { _ =>
      // There should be a single directory under the staging dir.
      val subdirs = tempDir.listFiles()
      assert(subdirs.length === 1)
      val stagingDir = subdirs(0).toURI().toString()

      val resultFile = new File(new URI(s"$stagingDir/${f.getName}"))
      resultFile.deleteOnExit()
      resultFile.exists() should be(true)
      Source.fromFile(resultFile).mkString should be("Test data")
    }
  }

  private def testJobSubmission(sid: Int, sync: Boolean): Unit = {
    val result = runJob(sid, new Echo(42), sync = sync)
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
