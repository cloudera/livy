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

package com.cloudera.livy.test

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.{Future => JFuture, TimeUnit}
import javax.servlet.http.HttpServletResponse

import scala.util.Try

import org.scalatest.BeforeAndAfterAll

import com.cloudera.livy.{LivyClient, LivyClientBuilder}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite
import com.cloudera.livy.test.jobs._

// Proper type representing the return value of "GET /sessions". At some point we should make
// SessionServlet use something like this.
private class SessionList {
  val from: Int = -1
  val total: Int = -1
  val sessions: List[SessionInfo] = Nil
}

class JobApiIT extends BaseIntegrationTestSuite with BeforeAndAfterAll {

  private var client: LivyClient = _
  private var sessionId: Int = _
  private var client2: LivyClient = _

  override def afterAll(): Unit = {
    super.afterAll()
    Seq(client, client2).foreach { c =>
      if (c != null) {
        c.stop(true)
      }
    }

    livyClient.stopSession(sessionId)
  }

  test("create a new session and upload test jar") {
    val tempClient = createClient(livyEndpoint)

    try {
      waitTillSessionIdle(sessionId)
      waitFor(tempClient.uploadJar(new File(testLib)))

      // Figure out the session ID by poking at the REST endpoint. We should probably expose this
      // in the Java API.
      val list = sessionList()
      assert(list.total === 1)
      sessionId = list.sessions(0).id

      client = tempClient
    } finally {
      if (client == null) {
        tempClient.stop(true)
      }
    }
  }

  test("upload file") {
    assume(client != null, "Client not active.")

    val file = Files.createTempFile("filetest", ".txt")
    Files.write(file, "hello".getBytes(UTF_8))

    waitFor(client.uploadFile(file.toFile()))

    val result = waitFor(client.submit(new FileReader(file.toFile().getName(), false)))
    assert(result === "hello")
  }

  test("run simple jobs") {
    assume(client != null, "Client not active.")

    val result = waitFor(client.submit(new Echo("hello")))
    assert(result === "hello")

    val result2 = waitFor(client.run(new Echo("hello")))
    assert(result2 === "hello")
  }

  test("run spark job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new SmallCount(100)));
    assert(result === 100)
  }

  test("run spark sql job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new SQLGetTweets(false)));
    assert(result.size() > 0)
  }

  test("stop a client without destroying the session") {
    assume(client != null, "Client not active.")
    client.stop(false)
    client = null
  }

  test("connect to an existing session") {
    assert(livyClient.getSessionStatus(sessionId) === SessionState.Idle().toString)
    val sessionUri = s"$livyEndpoint/sessions/$sessionId"
    client2 = createClient(sessionUri)
  }

  test("submit job using new client") {
    assume(client2 != null, "Client not active.")
    val result = waitFor(client2.submit(new Echo("hello")))
    assert(result === "hello")
  }

  test("run scala jobs") {
    assume(client2 != null, "Client not active.")

    val jobs = Seq(
      new ScalaEcho("abcde"),
      new ScalaEcho(Seq(1, 2, 3, 4)),
      new ScalaEcho(Map(1 -> 2, 3 -> 4)),
      new ScalaEcho(ValueHolder("abcde")),
      new ScalaEcho(ValueHolder(Seq(1, 2, 3, 4))),
      new ScalaEcho(Some("abcde"))
    )

    jobs.foreach { job =>
      val result = waitFor(client2.submit(job))
      assert(result === job.value)
    }
  }

  test("destroy the session") {
    assume(client2 != null, "Client not active.")
    client2.stop(true)

    val list = sessionList()
    assert(list.total === 0)

    val sessionUri = s"$livyEndpoint/sessions/$sessionId"
    Try(createClient(sessionUri)).foreach { client =>
      client.stop(true)
      fail("Should not have been able to connect to destroyed session.")
    }

    sessionId = -1
  }

  private def waitFor[T](future: JFuture[T]): T = {
    future.get(30, TimeUnit.SECONDS)
  }

  private def sessionList(): SessionList = {
    val response = httpClient.prepareGet(s"$livyEndpoint/sessions/").execute().get()
    assert(response.getStatusCode === HttpServletResponse.SC_OK)
    mapper.readValue(response.getResponseBodyAsStream, classOf[SessionList])
  }

  private def createClient(uri: String): LivyClient = {
    new LivyClientBuilder().setURI(new URI(uri)).build()
  }

}
