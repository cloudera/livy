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

import org.json4s.JsonAST._
import org.json4s.jackson.Json4sScalaModule

import com.cloudera.livy.ExecuteRequest
import com.cloudera.livy.server.BaseSessionServletSpec
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.interactive.{InteractiveSession, Statement}
import com.cloudera.livy.spark.{SparkProcess, SparkProcessBuilderFactory}
import com.cloudera.livy.spark.interactive.{CreateInteractiveRequest, InteractiveSessionFactory}

class InteractiveSessionServletSpec
  extends BaseSessionServletSpec[InteractiveSession, CreateInteractiveRequest] {

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  class MockInteractiveSession(id: Int, owner: String) extends InteractiveSession(id, owner) {
    var _state: SessionState = SessionState.Idle()

    var _idCounter = new AtomicInteger()
    var _statements = IndexedSeq[Statement]()

    override def kind: Kind = Spark()

    override def logLines(): IndexedSeq[String] = IndexedSeq()

    override def state: SessionState = _state

    override def stop(): Future[Unit] = Future.successful(())

    override def url_=(url: URL): Unit = throw new UnsupportedOperationException()

    override def executeStatement(executeRequest: ExecuteRequest): Statement = {
      val id = _idCounter.getAndIncrement
      val statement = new Statement(
        id,
        executeRequest,
        Future.successful(JObject(JField("value", JInt(42)))))

      _statements :+= statement

      statement
    }

    override def proxyUser: Option[String] = None

    override def url: Option[URL] = throw new UnsupportedOperationException()

    override def statements: IndexedSeq[Statement] = _statements

    override def interrupt(): Future[Unit] = throw new UnsupportedOperationException()
  }

  class MockInteractiveSessionFactory(processFactory: SparkProcessBuilderFactory)
    extends InteractiveSessionFactory(processFactory) {

    override def create(
        id: Int,
        owner: String,
        request: CreateInteractiveRequest): InteractiveSession = {
      new MockInteractiveSession(id, null)
    }

    protected override def create(
        id: Int,
        owner: String,
        process: SparkProcess,
        request: CreateInteractiveRequest): InteractiveSession = {
      throw new UnsupportedOperationException()
    }
  }

  override def sessionFactory: InteractiveSessionFactory = new MockInteractiveSessionFactory(
    new SparkProcessBuilderFactory(livyConf))

  override def servlet: InteractiveSessionServlet = new InteractiveSessionServlet(sessionManager)

  it("should setup and tear down an interactive session") {
    jget[Map[String, Any]]("/") { data =>
      data("sessions") should equal(Seq())
    }

    val createRequest = new CreateInteractiveRequest()
    createRequest.kind = Spark()

    jpost[Map[String, Any]]("/", createRequest) { data =>
      header("Location") should equal("/0")
      data("id") should equal (0)

      val session = sessionManager.get(0)
      session should be (defined)
    }

    jget[Map[String, Any]]("/0") { data =>
      data("id") should equal (0)
      data("state") should equal ("idle")

      val batch = sessionManager.get(0)
      batch should be (defined)
    }

    jpost[Map[String, Any]]("/0/statements", ExecuteRequest("foo")) { data =>
      data("id") should be (0)
      data("output") should be (Map("value" -> 42))
    }

    jget[Map[String, Any]]("/0/statements") { data =>
      data("total_statements") should be (1)
      data("statements").asInstanceOf[Seq[Map[String, Any]]](0)("id") should be (0)
    }

    jdelete[Map[String, Any]]("/0") { data =>
      data should equal (Map("msg" -> "deleted"))

      val session = sessionManager.get(0)
      session should not be defined
    }
  }

}
