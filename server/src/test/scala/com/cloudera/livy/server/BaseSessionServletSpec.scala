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

package com.cloudera.livy.server

import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatra.test.scalatest.ScalatraSuite

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{Session, SessionFactory, SessionManager}

abstract class BaseSessionServletSpec[S <: Session] extends ScalatraSuite
  with FunSpecLike with BeforeAndAfterAll {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  override protected def withFixture(test: NoArgTest) = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")
    test()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sessionManager.shutdown()
  }


  def sessionFactory: SessionFactory[S]
  def servlet: SessionServlet[S]

  val livyConf = new LivyConf()
  val sessionManager = new SessionManager(livyConf, sessionFactory)

  addServlet(servlet, "/*")

  protected def toJson(msg: AnyRef): String = write(msg)

  protected def headers: Map[String, String] = Map("Content-Type" -> "application/json")

  protected def getJson(uri: String, expectedStatus: Int = 200)(fn: (JValue) => Unit): Unit = {
    get(uri)(doTest(expectedStatus, fn))
  }

  protected def postJson(uri: String, body: AnyRef, expectedStatus: Int = 201)
      (fn: (JValue) => Unit): Unit = {
    post(uri, body = toJson(body), headers = headers)(doTest(expectedStatus, fn))
  }

  protected def deleteJson(uri: String, expectedStatus: Int = 200)(fn: (JValue) => Unit): Unit = {
    delete(uri)(doTest(expectedStatus, fn))
  }

  private def doTest(expectedStatus: Int, fn: (JValue) => Unit): Unit = {
    status should be (expectedStatus)
    header("Content-Type") should include("application/json")
    fn(parse(body))
  }

}
