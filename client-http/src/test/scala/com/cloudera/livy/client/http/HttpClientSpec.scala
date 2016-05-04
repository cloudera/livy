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

package com.cloudera.livy.client.http

import java.io.{File, InputStream}
import java.net.{InetAddress, URI}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Future => JFuture, _}
import java.util.concurrent.atomic.AtomicLong
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener

import com.cloudera.livy._
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.WebServer
import com.cloudera.livy.server.interactive.{InteractiveSession, InteractiveSessionServlet}
import com.cloudera.livy.sessions.{SessionState, Spark}
import com.cloudera.livy.test.jobs.Echo

/**
 * The test for the HTTP client is written in Scala so we can reuse the code in the livy-server
 * module, which implements the client session backend. The client servlet has some functionality
 * overridden to avoid creating sub-processes for each seession.
 */
class HttpClientSpec extends FunSpecLike with BeforeAndAfterAll {

  import HttpClientSpec._

  private val TIMEOUT_S = 10
  private val ID_GENERATOR = new AtomicLong()
  private val serializer = new Serializer()

  private var server: WebServer = _
  private var client: LivyClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = new WebServer(new LivyConf(), "0.0.0.0", 0)

    server.context.setResourceBase("src/main/com/cloudera/livy/server")
    server.context.setInitParameter(ScalatraListener.LifeCycleKey,
      classOf[HttpClientTestBootstrap].getCanonicalName)
    server.context.addEventListener(new ScalatraListener)

    server.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (server != null) {
      server.stop()
      server = null
    }
    if (client != null) {
      client.stop(true)
      client = null
    }
    session = null
  }

  describe("HTTP client library") {

    it("should create clients") {
      // WebServer does this internally instad of respecting "0.0.0.0", so try to use the same
      // address.
      val uri = s"http://${InetAddress.getLocalHost.getHostAddress}:${server.port}/"
      client = new LivyClientBuilder(false).setURI(new URI(uri)).build()
    }

    withClient("should run and monitor asynchronous jobs") {
      testJob(false)
    }

    withClient("should propagate errors from jobs") {
      val errorMessage = "This job throws an error."
      val (jobId, handle) = runJob(false, { id => Seq(
          new JobStatus(id, JobHandle.State.FAILED, null, errorMessage))
        })

      val error = intercept[ExecutionException] {
        handle.get(TIMEOUT_S, TimeUnit.SECONDS)
      }
      assert(error.getCause() != null)
      assert(error.getCause().getMessage().indexOf(errorMessage) >= 0)
      verify(session, times(1)).jobStatus(meq(jobId))
    }

    withClient("should run and monitor synchronous jobs") {
      testJob(false)
    }

    withClient("should add files and jars") {
      val furi = new URI("hdfs:file")
      val juri = new URI("hdfs:jar")

      client.addFile(furi).get(TIMEOUT_S, TimeUnit.SECONDS)
      client.addJar(juri).get(TIMEOUT_S, TimeUnit.SECONDS)

      verify(session, times(1)).addFile(meq(furi))
      verify(session, times(1)).addJar(meq(juri))
    }

    withClient("should upload files and jars") {
      uploadAndVerify("file")
      uploadAndVerify("jar")
    }

    withClient("should cancel jobs") {
      val (jobId, handle) = runJob(false, { id => Seq(
          new JobStatus(id, JobHandle.State.STARTED, null, null),
          new JobStatus(id, JobHandle.State.CANCELLED, null, null))
        })
      handle.cancel(true)

      intercept[CancellationException] {
        handle.get(TIMEOUT_S, TimeUnit.SECONDS)
      }

      verify(session, times(1)).cancelJob(meq(jobId))
    }

    withClient("should notify listeners of new Spark jobs") {
      val (jobId, handle) = runJob(false, { id => Seq(
          new JobStatus(id, JobHandle.State.STARTED, null, null),
          new JobStatus(id, JobHandle.State.SUCCEEDED, serialize(id), null))
        })

      val listener = mock(classOf[JobHandle.Listener[Long]])
      handle.asInstanceOf[JobHandle[Long]].addListener(listener)

      assert(handle.get(TIMEOUT_S, TimeUnit.SECONDS) === jobId)
      verify(listener, times(1)).onJobSucceeded(any(), any())
    }

    withClient("should time out handle get() call") {
      // JobHandleImpl does exponential backoff checking the result of a job. Given an initial
      // wait of 100ms, 4 iterations should result in a wait of 800ms, so the handle should at that
      // point timeout a wait of 100ms.
      val (jobId, handle) = runJob(false, { id => Seq(
          new JobStatus(id, JobHandle.State.STARTED, null, null),
          new JobStatus(id, JobHandle.State.STARTED, null, null),
          new JobStatus(id, JobHandle.State.STARTED, null, null),
          new JobStatus(id, JobHandle.State.SUCCEEDED, serialize(id), null))
        })

      intercept[TimeoutException] {
        handle.get(100, TimeUnit.MILLISECONDS)
      }

      assert(handle.get(TIMEOUT_S, TimeUnit.SECONDS) === jobId)
    }

    withClient("should handle null responses") {
      testJob(false, response = Some(null))
    }

    withClient("should connect to existing sessions") {
      var sid = client.asInstanceOf[HttpClient].getSessionId()
      val uri = s"http://${InetAddress.getLocalHost.getHostAddress}:${server.port}" +
        s"${LivyConnection.SESSIONS_URI}/$sid"
      val newClient = new LivyClientBuilder(false).setURI(new URI(uri)).build()
      newClient.stop(false)
      verify(session, never()).stop()
    }

    withClient("should tear down clients") {
      client.stop(true)
      verify(session, times(1)).stop()
    }

  }

  private def uploadAndVerify(cmd: String): Unit = {
    val f = File.createTempFile("uploadTestFile", cmd)
    val expectedStr = "Test data"
    val expectedData = expectedStr.getBytes()
    Files.write(Paths.get(f.getAbsolutePath), expectedData)
    val b = new Array[Byte](expectedData.length)
    val captor = ArgumentCaptor.forClass(classOf[InputStream])
    if (cmd == "file") {
      client.uploadFile(f).get(TIMEOUT_S, TimeUnit.SECONDS)
      verify(session, times(1)).addFile(captor.capture(), meq(f.getName))
    } else {
      client.uploadJar(f).get(TIMEOUT_S, TimeUnit.SECONDS)
      verify(session, times(1)).addJar(captor.capture(), meq(f.getName))
    }
    captor.getValue.read(b)
    assert(expectedStr === new String(b))
  }

  private def runJob(sync: Boolean, genStatusFn: Long => Seq[JobStatus]): (Long, JFuture[Int]) = {
    val jobId = java.lang.Long.valueOf(ID_GENERATOR.incrementAndGet())
    when(session.submitJob(any(classOf[Array[Byte]]))).thenReturn(jobId)

    val statuses = genStatusFn(jobId)
    val first = statuses.head
    val remaining = statuses.drop(1)
    when(session.jobStatus(meq(jobId))).thenReturn(first, remaining: _*)

    val job = new Echo(42)
    val handle = if (sync) client.run(job) else client.submit(job)
    (jobId, handle)
  }

  private def testJob(sync: Boolean, response: Option[Any] = None): Unit = {
    val (jobId, handle) = runJob(sync, { id => Seq(
        new JobStatus(id, JobHandle.State.STARTED, null, null),
        new JobStatus(id, JobHandle.State.SUCCEEDED, serialize(response.getOrElse(id)), null))
      })
    assert(handle.get(TIMEOUT_S, TimeUnit.SECONDS) === response.getOrElse(jobId))
    verify(session, times(2)).jobStatus(meq(jobId))
  }

  private def withClient(desc: String)(fn: => Unit): Unit = {
    it(desc) {
      assume(client != null, "No active client.")
      fn
    }
  }

  def serialize(value: Any): Array[Byte] = {
    BufferUtils.toByteArray(serializer.serialize(value))
  }

}

private object HttpClientSpec {

  // Hack warning: keep the session object available so that individual tests can mock
  // the desired behavior before making requests to the server.
  var session: InteractiveSession = _

}

private class HttpClientTestBootstrap extends LifeCycle {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  override def init(context: ServletContext): Unit = {
    val servlet = new InteractiveSessionServlet(new LivyConf()) {
      override protected def createSession(req: HttpServletRequest): InteractiveSession = {
        val session = mock(classOf[InteractiveSession])
        val id = sessionManager.nextId()
        when(session.id).thenReturn(id)
        when(session.state).thenReturn(SessionState.Idle())
        when(session.proxyUser).thenReturn(None)
        when(session.kind).thenReturn(Spark())
        when(session.stop()).thenReturn(Future.successful(()))
        require(HttpClientSpec.session == null, "Session already created?")
        HttpClientSpec.session = session
        session
      }
    }

    context.mount(servlet, s"${LivyConnection.SESSIONS_URI}/*")
  }

}
