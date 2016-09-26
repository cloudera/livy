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

package com.cloudera.livy.sessions

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

import org.mockito.Mockito.{never, verify, when}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}
import com.cloudera.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import com.cloudera.livy.server.recovery.SessionStore

class SessionManagerSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {
  describe("SessionManager") {
    it("should garbage collect old sessions") {
      val livyConf = new LivyConf()
      livyConf.set(SessionManager.SESSION_TIMEOUT, "100ms")
      val manager = new SessionManager[MockSession](livyConf)
      val session = manager.register(new MockSession(manager.nextId(), null, livyConf))
      manager.get(session.id).isDefined should be(true)
      eventually(timeout(5 seconds), interval(100 millis)) {
        Await.result(manager.collectGarbage(), Duration.Inf)
        manager.get(session.id) should be(None)
      }
    }
  }

  describe("BatchSessionManager") {
    implicit def executor: ExecutionContext = ExecutionContext.global

    def makeMetadata(id: Int, appTag: String): BatchRecoveryMetadata = {
      BatchRecoveryMetadata(id, None, appTag, null, None)
    }

    def mockSession(id: Int): BatchSession = {
      val session = mock[BatchSession]
      when(session.id).thenReturn(id)
      when(session.stop()).thenReturn(Future {})
      when(session.lastActivity).thenReturn(System.nanoTime())

      session
    }

    it("should not fail if state store is empty") {
      val conf = new LivyConf()

      val sessionStore = mock[SessionStore]
      when(sessionStore.getAllSessions[BatchRecoveryMetadata]("batch"))
        .thenReturn(Seq.empty)

      val sm = new BatchSessionManager(conf, sessionStore)
      sm.nextId() shouldBe 0
    }

    it("should recover sessions from state store") {
      val conf = new LivyConf()
      conf.set(LivyConf.LIVY_SPARK_MASTER.key, "yarn-cluster")

      val sessionType = "batch"
      val nextId = 99

      val validMetadata = List(makeMetadata(0, "t1"), makeMetadata(77, "t2")).map(Try(_))
      val sessionStore = mock[SessionStore]
      when(sessionStore.getNextSessionId(sessionType)).thenReturn(nextId)
      when(sessionStore.getAllSessions[BatchRecoveryMetadata](sessionType))
        .thenReturn(validMetadata)

      val sm = new BatchSessionManager(conf, sessionStore)
      sm.nextId() shouldBe nextId
      validMetadata.foreach { m =>
        sm.get(m.get.id) shouldBe defined
      }
      sm.size shouldBe validMetadata.size
    }

    it("should delete sessions from state store") {
      val conf = new LivyConf()

      val sessionType = "batch"
      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined

      Await.ready(sm.delete(sessionId).get, 30 seconds)

      verify(sessionStore).remove(sessionType, sessionId)
      sm.get(sessionId) shouldBe None
    }

    it("should delete sessions on shutdown when recovery is off") {
      val conf = new LivyConf()
      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined
      sm.shutdown()

      verify(session).stop()
    }

    it("should not delete sessions on shutdown with recovery is on") {
      val conf = new LivyConf()
      conf.set(LivyConf.RECOVERY_MODE, SessionManager.SESSION_RECOVERY_MODE_RECOVERY)

      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined
      sm.shutdown()

      verify(session, never).stop()
    }
  }
}
