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
import java.nio.file.{Paths, Files}
import java.util.{ArrayList, HashMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.apache.spark.api.java.function.VoidFunction
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{Job, JobContext, JobHandle}
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.BaseSessionServletSpec
import com.cloudera.livy.spark.client._

class ClientServletSpec extends BaseSessionServletSpec[ClientSession, CreateClientRequest] {

  override protected def withFixture(test: NoArgTest) = {
    test()
  }

  override def sessionFactory = new ClientSessionFactory()

  override def servlet = new ClientSessionServlet(sessionManager)

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

      jpost[SessionInfo]("/", new CreateClientRequest(10000L, conf)) { data =>
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
  }

  private def testResourceUpload(cmd: String, sessionId: Int): Unit = {
    val f = File.createTempFile("uploadTestFile", cmd)

    Files.write(Paths.get(f.getAbsolutePath), "Test data".getBytes())

    jpost[Unit](s"/$sessionId/upload-$cmd", Map(cmd -> f), 200) { _ =>
      val resultFile = new File(new URI(s"${sessionManager.livyHome}/$sessionId/${f.getName}"))
      resultFile.deleteOnExit()
      resultFile.exists() should be(true)
      Source.fromFile(resultFile).mkString should be("Test data")
    }
  }
  private def testJobSubmission(sid: Int, sync: Boolean): Unit = {
    val ser = new Serializer()
    val job = BufferUtils.toByteArray(ser.serialize(new TestJob()))
    val route = if (sync) s"/$sid/submit-job" else s"/$sid/run-job"
    var jobId: Long = -1L
    jpost[JobStatus](route, new SerializedJob(job)) { data =>
      jobId = data.id
    }
    eventually(timeout(1 minute), interval(100 millis)) {
      jget[JobStatus](s"/$sid/jobs/$jobId") { status =>
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
