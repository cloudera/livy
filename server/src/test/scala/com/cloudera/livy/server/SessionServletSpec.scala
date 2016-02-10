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

import scala.concurrent.Future

import com.cloudera.livy.sessions.{Session, SessionFactory, SessionManager, SessionState}

object SessionServletSpec {

  val REMOTE_USER_HEADER = "X-Livy-SessionServlet-User"

  class MockSession(id: Int, owner: String) extends Session(id, owner) {

    def state: SessionState = SessionState.Idle()

    def stop(): Future[Unit] = Future.successful()

    def logLines(): IndexedSeq[String] = IndexedSeq("log")

  }

  case class MockSessionView(id: Int, owner: String, logs: Seq[String])

}

class SessionServletSpec
  extends BaseSessionServletSpec[Session, Map[String, String]](needsSpark = false) {

  import SessionServletSpec._

  def sessionFactory: SessionFactory[Session, Map[String, String]] = {
    new SessionFactory[Session, Map[String, String]]() {
      override def create(id: Int, owner: String, createRequest: Map[String, String]): Session = {
        new MockSession(id, owner)
      }
    }
  }

  def servlet: SessionServlet[Session, Map[String, String]] = {
    new SessionServlet(sessionManager) {
      override protected def clientSessionView(session: Session, req: HttpServletRequest): Any = {
        val logs = if (isOwner(session, req)) session.logLines() else Nil
        MockSessionView(session.id, session.owner, logs)
      }

      override protected def remoteUser(req: HttpServletRequest): String = {
        req.getHeader(REMOTE_USER_HEADER)
      }
    }
  }

  private val aliceHeaders = defaultHeaders ++ Map(REMOTE_USER_HEADER -> "alice")
  private val bobHeaders = defaultHeaders ++ Map(REMOTE_USER_HEADER -> "bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {

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

  }

}
