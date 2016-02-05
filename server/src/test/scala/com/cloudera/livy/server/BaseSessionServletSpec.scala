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

import org.scalatest.BeforeAndAfterAll

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{Session, SessionFactory, SessionManager}

abstract class BaseSessionServletSpec[S <: Session, R]
  extends BaseJsonServletSpec
  with BeforeAndAfterAll {

  val Sessions = "sessions"
  val Location = "Location"

  override protected def withFixture(test: NoArgTest) = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")
    test()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sessionManager.shutdown()
  }

  def sessionFactory: SessionFactory[S, R]
  def servlet: SessionServlet[S, R]
  val livyConf = new LivyConf()
  lazy val sessionManager = new SessionManager(livyConf, sessionFactory)

  addServlet(servlet, "/*")

  protected def toJson(msg: AnyRef): Array[Byte] = mapper.writeValueAsBytes(msg)

}
