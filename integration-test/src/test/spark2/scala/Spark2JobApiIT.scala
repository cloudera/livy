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
import java.util.concurrent.{TimeUnit, Future => JFuture}
import javax.servlet.http.HttpServletResponse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.BeforeAndAfterAll

import com.cloudera.livy._
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.sessions.SessionKindModule
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite
import com.cloudera.livy.test.jobs.spark2._

class Spark2JobApiIT extends BaseIntegrationTestSuite with BeforeAndAfterAll with Logging {

  private var client: LivyClient = _
  private var sessionId: Int = _
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  override def afterAll(): Unit = {
    super.afterAll()

    if (client != null) {
      client.stop(true)
    }

    livyClient.connectSession(sessionId).stop()
  }

  test("create a new session and upload test jar") {
    val tempClient = createClient(livyEndpoint)

    try {
      // Figure out the session ID by poking at the REST endpoint. We should probably expose this
      // in the Java API.
      val list = sessionList()
      assert(list.total === 1)
      val tempSessionId = list.sessions(0).id

      livyClient.connectSession(tempSessionId).verifySessionIdle()
      waitFor(tempClient.uploadJar(new File(testLib)))

      client = tempClient
      sessionId = tempSessionId
    } finally {
      if (client == null) {
        try {
          if (tempClient != null) {
            tempClient.stop(true)
          }
        } catch {
          case e: Exception => warn("Error stopping client.", e)
        }
      }
    }
  }

  test("run spark2 job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new SparkSessionTest()))
    assert(result === 3)
  }

  test("run spark2 dataset job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new DatasetTest()))
    assert(result === 2)
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
