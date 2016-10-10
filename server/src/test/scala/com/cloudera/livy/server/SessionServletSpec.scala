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
import javax.servlet.http.HttpServletResponse._

import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.LivyConf
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions.{Session, SessionManager, SessionState}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

object SessionServletSpec {

  val PROXY_USER = "proxyUser"

  class MockSession(id: Int, owner: String, livyConf: LivyConf)
    extends Session(id, owner, livyConf) {

    case class MockRecoveryMetadata(id: Int) extends RecoveryMetadata()

    override val proxyUser = None

    override def recoveryMetadata: RecoveryMetadata = MockRecoveryMetadata(0)

    override def state: SessionState = SessionState.Idle()

    override protected def stopSession(): Unit = ()

    override def logLines(): IndexedSeq[String] = IndexedSeq("log")

  }

  case class MockSessionView(id: Int, owner: String, logs: Seq[String])

}

class SessionServletSpec
  extends BaseSessionServletSpec[Session, RecoveryMetadata] {

  import SessionServletSpec._

  override def createServlet(): SessionServlet[Session, RecoveryMetadata] = {
    val conf = createConf()
    val sessionManager = new SessionManager[Session, RecoveryMetadata](
      conf,
      { _ => assert(false).asInstanceOf[Session] },
      mock[SessionStore],
      "test",
      Some(Seq.empty))

    new SessionServlet(sessionManager, conf) with RemoteUserOverride {
      override protected def createSession(req: HttpServletRequest): Session = {
        val params = bodyAs[Map[String, String]](req)
        checkImpersonation(params.get(PROXY_USER), req)
        new MockSession(sessionManager.nextId(), remoteUser(req), conf)
      }

      override protected def clientSessionView(
          session: Session,
          req: HttpServletRequest): Any = {
        val logs = if (hasAccess(session.owner, req)) session.logLines() else Nil
        MockSessionView(session.id, session.owner, logs)
      }
    }
  }

  private val aliceHeaders = makeUserHeaders("alice")
  private val bobHeaders = makeUserHeaders("bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {

    it("should return correct Location in header") {
      // mount to "/sessions/*" to test. If request URI is "/session", getPathInfo() will
      // return null, since there's no extra path.
      // mount to "/*" will always return "/", so that it cannot reflect the issue.
      addServlet(servlet, "/sessions/*")
      jpost[MockSessionView]("/sessions", Map(), headers = aliceHeaders) { res =>
        assert(header("Location") === "/sessions/0")
        jdelete[Map[String, Any]]("/sessions/0", SC_OK, aliceHeaders) { _ => }
      }
    }

    it("should attach owner information to sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        assert(res.owner === "alice")
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should allow other users to see non-sensitive information") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.logs === Nil)
        }
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should prevent non-owners from modifying sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
      }
    }

    it("should allow admins to access all sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = adminHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, adminHeaders, SC_OK)
      }
    }

    it("should not allow regular users to impersonate others") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }
    }

    it("should allow admins to impersonate anyone") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = adminHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
        delete(res.id, adminHeaders, SC_OK)
      }
    }

  }

}
