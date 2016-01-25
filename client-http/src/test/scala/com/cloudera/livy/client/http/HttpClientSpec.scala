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

import java.net.{InetAddress, URI}
import java.util.concurrent.{ExecutionException, Future => JFuture, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import javax.servlet.ServletContext

import scala.concurrent.{ExecutionContext, Future}

import org.json4s.JValue
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener

import com.cloudera.livy._
import com.cloudera.livy.client.common.{BufferUtils, Serializer}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.client.ClientSessionServlet
import com.cloudera.livy.sessions.{SessionFactory, SessionManager, SessionState}
import com.cloudera.livy.spark.client.ClientSession

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
      client.stop()
      client= null
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

    withClient("should tear down clients") {
      client.stop()
      verify(session, times(1)).stop()
    }

  }

  private def runJob(sync: Boolean, genStatusFn: Long => Seq[JobStatus]): (Long, JFuture[Long]) = {
    val jobId = java.lang.Long.valueOf(ID_GENERATOR.incrementAndGet())
    when(session.submitJob(any(classOf[Array[Byte]]))).thenReturn(jobId)

    val statuses = genStatusFn(jobId)
    val first = statuses.head
    val remaining = statuses.drop(1)
    when(session.jobStatus(meq(jobId))).thenReturn(first, remaining: _*)

    val handle = if (sync) client.run(new DummyJob()) else client.submit(new DummyJob())
      (jobId, handle)
  }

  private def testJob(sync: Boolean): Unit = {
    val (jobId, handle) = runJob(sync, { id => Seq(
        new JobStatus(id, JobHandle.State.STARTED, null, null),
        new JobStatus(id, JobHandle.State.SUCCEEDED, serialize(id), null))
      })
    assert(handle.get(TIMEOUT_S, TimeUnit.SECONDS) === jobId)
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

// Won't really get called, here just so we can use the API.
class DummyJob extends Job[Long] {

  override def call(jc: JobContext): Long = 42L

}

private object HttpClientSpec {

  // Hack warning: keep the session object available so that individual tests can mock
  // the desired behavior before making requests to the server.
  var session: ClientSession = _

}

private class HttpClientTestBootstrap extends LifeCycle {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  override def init(context: ServletContext): Unit = {
    val factory = mock(classOf[SessionFactory[ClientSession]])
    when(factory.create(anyInt(), any(classOf[JValue]))).thenAnswer(new Answer[ClientSession]() {
      override def answer(args: InvocationOnMock): ClientSession = {
        val session = mock(classOf[ClientSession])
        val id = args.getArguments()(0).asInstanceOf[Int]
        when(session.id).thenReturn(id)
        when(session.state).thenReturn(SessionState.Idle())
        when(session.stop()).thenReturn(Future { })
        require(HttpClientSpec.session == null, "Session already created?")
        HttpClientSpec.session = session
        session
      }
    })

    val mgr = new SessionManager(new LivyConf(), factory)
    context.mount(new ClientSessionServlet(mgr), "/clients/*")
  }

}
