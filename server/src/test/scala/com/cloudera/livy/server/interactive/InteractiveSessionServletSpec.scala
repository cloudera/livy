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

import java.net.URL
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.{JArray, JInt, JObject, JString}

import com.cloudera.livy.ExecuteRequest
import com.cloudera.livy.server.BaseSessionServletSpec
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.interactive.{InteractiveSession, Statement}
import com.cloudera.livy.spark.interactive.{CreateInteractiveRequest, InteractiveSessionFactory}
import com.cloudera.livy.spark.{SparkProcess, SparkProcessBuilderFactory}

class InteractiveSessionServletSpec extends BaseSessionServletSpec[InteractiveSession] {

  override protected implicit lazy val jsonFormats: Formats = DefaultFormats ++ Serializers.Formats

  class MockInteractiveSession(val id: Int) extends InteractiveSession {
    var _state: SessionState = SessionState.Idle()

    var _idCounter = new AtomicInteger()
    var _statements = IndexedSeq[Statement]()

    override def kind: Kind = Spark()

    override def logLines() = IndexedSeq()

    override def state = _state

    override def stop(): Future[Unit] = Future.successful(())

    override def url_=(url: URL): Unit = ???

    override def executeStatement(executeRequest: ExecuteRequest): Statement = {
      val id = _idCounter.getAndIncrement
      val statement = new Statement(
        id,
        executeRequest,
        Future.successful(JObject()))

      _statements :+= statement

      statement
    }

    override def proxyUser: Option[String] = None

    override def url: Option[URL] = ???

    override def statements: IndexedSeq[Statement] = _statements

    override def interrupt(): Future[Unit] = ???
  }

  class MockInteractiveSessionFactory(processFactory: SparkProcessBuilderFactory)
    extends InteractiveSessionFactory(processFactory) {

    override def create(id: Int, request: CreateInteractiveRequest): InteractiveSession = {
      new MockInteractiveSession(id)
    }

    protected override def create(id: Int,
                                  process: SparkProcess,
                                  request: CreateInteractiveRequest): InteractiveSession = {
      throw new UnsupportedOperationException()
    }
  }

  override def sessionFactory = new MockInteractiveSessionFactory(
    new SparkProcessBuilderFactory(livyConf))

  override def servlet = new InteractiveSessionServlet(sessionManager)

  it("should setup and tear down an interactive session") {
    getJson("/") { data =>
      data \ "sessions" should equal(JArray(List()))
    }

    postJson("/", CreateInteractiveRequest(kind = Spark())) { data =>
      header("Location") should equal("/0")
      data \ "id" should equal (JInt(0))

      val session = sessionManager.get(0)
      session should be (defined)
    }

    getJson("/0") { data =>
      data \ "id" should equal (JInt(0))
      data \ "state" should equal (JString("idle"))

      val batch = sessionManager.get(0)
      batch should be (defined)
    }

    deleteJson("/0") { data =>
      data should equal (JObject(("msg", JString("deleted"))))

      val session = sessionManager.get(0)
      session should not be defined
    }
  }

}
