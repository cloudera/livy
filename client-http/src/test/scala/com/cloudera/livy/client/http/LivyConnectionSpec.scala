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

import java.net.URLEncoder
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.http.client.utils.URIBuilder
import org.eclipse.jetty.security._
import org.eclipse.jetty.security.authentication.BasicAuthenticator
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.security._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatest.Matchers._

import com.cloudera.livy.LivyBaseUnitTestSuite

class LivyConnectionSpec extends FunSpecLike with BeforeAndAfterAll with LivyBaseUnitTestSuite {
  describe("LivyConnection") {
    def basicAuth(username: String, password: String, realm: String): SecurityHandler = {
      val roles = Array("user")

      val l = new HashLoginService()
      l.putUser(username, Credential.getCredential(password), roles)
      l.setName(realm)

      val constraint = new Constraint()
      constraint.setName(Constraint.__BASIC_AUTH)
      constraint.setRoles(roles)
      constraint.setAuthenticate(true)

      val cm = new ConstraintMapping()
      cm.setConstraint(constraint)
      cm.setPathSpec("/*")

      val csh = new ConstraintSecurityHandler()
      csh.setAuthenticator(new BasicAuthenticator())
      csh.setRealmName(realm)
      csh.addConstraintMapping(cm)
      csh.setLoginService(l)

      csh
    }

    def staticServlet(): HttpServlet = new HttpServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.getWriter.print("true")
      }
    }

    it("should support HTTP auth") {
      val username = "user name"
      val password = "pass:word"

      val server = new Server(0)
      val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
      context.setSecurityHandler(basicAuth(username, password, "realm"))
      context.setContextPath("/")
      context.addServlet(new ServletHolder(staticServlet()), "/")
      server.setHandler(context)
      server.start()

      val uri = new URIBuilder(server.getURI())
        .setUserInfo(URLEncoder.encode(username, "UTF-8"), URLEncoder.encode(password, "UTF-8"))
        .build()
      info(uri.toString)
      val conn = new LivyConnection(uri, new HttpConf(null))
      try {
        conn.get(classOf[Boolean], "/") shouldBe true
      } finally {
        conn.close()
      }

      server.stop()
      server.join()
    }
  }
}
