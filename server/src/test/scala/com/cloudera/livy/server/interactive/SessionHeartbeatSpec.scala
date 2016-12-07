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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.Mockito.{never, verify, when}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.LivyConf
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions.{Session, SessionManager}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

class SessionHeartbeatSpec extends FunSpec with Matchers {
  describe("SessionHeartbeat") {
    class TestHeartbeat(override val heartbeatTimeout: FiniteDuration) extends SessionHeartbeat {}

    it("should not expire if heartbeat was never called.") {
      val t = new TestHeartbeat(Duration.Zero)
      t.heartbeatExpired shouldBe false
    }

    it("should expire if time has elapsed.") {
      val t = new TestHeartbeat(Duration.fromNanos(1))
      t.heartbeat()
      eventually(timeout(2 nano), interval(1 nano)) {
        t.heartbeatExpired shouldBe true
      }
    }

    it("should not expire if time hasn't elapsed.") {
      val t = new TestHeartbeat(Duration.create(1, DAYS))
      t.heartbeat()
      t.heartbeatExpired shouldBe false
    }
  }

  describe("SessionHeartbeatWatchdog") {
    abstract class TestSession extends Session(0, null, null) with SessionHeartbeat {}
    class TestWatchdog(conf: LivyConf)
      extends SessionManager[TestSession, RecoveryMetadata](
        conf,
        { _ => assert(false).asInstanceOf[TestSession] },
        mock[SessionStore],
        "test",
        Some(Seq.empty))
        with SessionHeartbeatWatchdog[TestSession, RecoveryMetadata] {}

    it("should delete only expired sessions") {
      val expiredSession: TestSession = mock[TestSession]
      when(expiredSession.id).thenReturn(0)
      when(expiredSession.heartbeatExpired).thenReturn(true)

      val nonExpiredSession: TestSession = mock[TestSession]
      when(nonExpiredSession.id).thenReturn(1)
      when(nonExpiredSession.heartbeatExpired).thenReturn(false)

      val n = new TestWatchdog(new LivyConf())

      n.register(expiredSession)
      n.register(nonExpiredSession)
      n.deleteExpiredSessions()

      verify(expiredSession).stop()
      verify(nonExpiredSession, never).stop()
    }
  }
}
