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
package com.cloudera.livy.server.recovery

import scala.util.Try

import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.LivyConf
import com.cloudera.livy.server.batch.BatchRecoveryMetadata
import com.cloudera.livy.sessions.MockSession
import com.cloudera.livy.sessions.Session.RecoveryMetadata

class SessionRecoverySpec extends FunSpec {
  case class TestRecoveryMetadata(id: Int) extends RecoveryMetadata

  describe("SessionRecovery") {
    val conf = new LivyConf()

    it("should not return error on recovering from empty state store") {
      val sessionStore = mock[SessionStore]
      when(sessionStore.getAllSessions("batch", classOf[BatchRecoveryMetadata]))
        .thenReturn(Seq.empty)
      val recovery = new SessionRecovery(sessionStore, conf)

      val mgr = recovery.recoverBatchSessions()
      mgr.nextId() shouldBe 0
    }

    it("should recover sessions") {
      val sessionType = "test"
      val nextId = 72

      val validMetadata = List(TestRecoveryMetadata(0), TestRecoveryMetadata(77)).map(Try(_))
      val sessionStore = mock[SessionStore]
      when(sessionStore.getNextSessionId(sessionType)).thenReturn(nextId)
      when(sessionStore.getAllSessions(sessionType, classOf[TestRecoveryMetadata]))
        .thenReturn(validMetadata)
      val recovery = new SessionRecovery(sessionStore, conf)

      val mgr = recovery.recover(sessionType, classOf[TestRecoveryMetadata]) { m =>
        new MockSession(m.id, null, conf)
      }

      mgr.nextId() shouldBe nextId
      mgr.all().map(_.id) should contain theSameElementsAs validMetadata.map(_.get.id)
    }
  }
}
