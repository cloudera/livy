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

import javax.servlet.http.HttpServletRequest

import org.scalatest.BeforeAndAfterAll

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.Session

object BaseSessionServletSpec {

  /** Header used to override the user remote user in tests. */
  val REMOTE_USER_HEADER = "X-Livy-SessionServlet-User"

}

abstract class BaseSessionServletSpec[S <: Session]
  extends BaseJsonServletSpec
  with BeforeAndAfterAll {

  /** Config map containing option that is blacklisted. */
  protected val BLACKLISTED_CONFIG = Map("spark.do_not_set" -> "true")

  /** Name of the admin user. */
  protected val ADMIN = "__admin__"

  /** Create headers that identify a specific user in tests. */
  protected def makeUserHeaders(user: String): Map[String, String] = {
    defaultHeaders ++ Map(BaseSessionServletSpec.REMOTE_USER_HEADER -> user)
  }

  protected val adminHeaders = makeUserHeaders(ADMIN)

  /** Create a LivyConf with impersonation enabled and a superuser. */
  protected def createConf(): LivyConf = {
    new LivyConf()
      .set(LivyConf.IMPERSONATION_ENABLED, true)
      .set(LivyConf.SUPERUSERS, ADMIN)
      .set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    servlet.shutdown()
  }

  def createServlet(): SessionServlet[S]

  protected val servlet = createServlet()

  addServlet(servlet, "/*")

  protected def toJson(msg: AnyRef): Array[Byte] = mapper.writeValueAsBytes(msg)

}

trait RemoteUserOverride {
  this: SessionServlet[_] =>

  override protected def remoteUser(req: HttpServletRequest): String = {
    req.getHeader(BaseSessionServletSpec.REMOTE_USER_HEADER)
  }

}
