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

import java.io.IOException
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.http.client.utils.URIBuilder
import org.eclipse.jetty.security._
import org.eclipse.jetty.security.authentication.BasicAuthenticator
import org.eclipse.jetty.util.security._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import org.scalatest.Matchers._
import org.scalatra.servlet.ScalatraListener

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}
import com.cloudera.livy.server.WebServer

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

    def test(password: String, livyConf: LivyConf = new LivyConf()): Unit = {
      val username = "user name"

      val server = new WebServer(livyConf, "0.0.0.0", 0)
      server.context.setSecurityHandler(basicAuth(username, password, "realm"))
      server.context.setResourceBase("src/main/com/cloudera/livy/server")
      server.context.setInitParameter(ScalatraListener.LifeCycleKey,
        classOf[HttpClientTestBootstrap].getCanonicalName)
      server.context.addEventListener(new ScalatraListener)
      server.start()

      val utf8Name = UTF_8.name()
      val uri = new URIBuilder()
        .setScheme(server.protocol)
        .setHost(server.host)
        .setPort(server.port)
        .setUserInfo(URLEncoder.encode(username, utf8Name), URLEncoder.encode(password, utf8Name))
        .build()
      info(uri.toString)
      val conn = new LivyConnection(uri, new HttpConf(null))
      try {
        conn.get(classOf[Object], "/") should not be (null)

      } finally {
        conn.close()
      }

      server.stop()
      server.join()
    }

    it("should support HTTP auth with password") {
      test("pass:word")
    }

    it("should support HTTP auth with empty password") {
      test("")
    }

    it("should be failed with large header size") {
      val livyConf = new LivyConf()
        .set(LivyConf.REQUEST_HEADER_SIZE, 1024)
        .set(LivyConf.RESPONSE_HEADER_SIZE, 1024)
      val pwd = "test-password" * 100
      val exception = intercept[IOException](test(pwd, livyConf))
      exception.getMessage.contains("Request Entity Too Large") should be(true)
    }

    it("should be succeeded with configured header size") {
      val livyConf = new LivyConf()
        .set(LivyConf.REQUEST_HEADER_SIZE, 2048)
        .set(LivyConf.RESPONSE_HEADER_SIZE, 2048)
      val pwd = "test-password" * 100
      test(pwd, livyConf)
    }
  }
}
