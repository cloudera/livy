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

import scala.util.Success

import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

class SessionStoreSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("SessionStore") {
    case class TestRecoveryMetadata(id: Int) extends RecoveryMetadata

    val sessionType = "test"
    val sessionPath = s"v1/$sessionType"
    val sessionManagerPath = s"v1/$sessionType/state"

    val conf = new LivyConf()
    it("should set session state and session counter when saving a session.") {
      val stateStore = mock[StateStore]
      val sessionStore = new SessionStore(conf, stateStore)

      val m = TestRecoveryMetadata(99)
      sessionStore.save(sessionType, m)
      verify(stateStore).set(s"$sessionPath/99", m)
    }

    it("should return existing sessions") {
      val validMetadata = Map(
        "0" -> Some(TestRecoveryMetadata(0)),
        "5" -> None,
        "77" -> Some(TestRecoveryMetadata(77)))
      val corruptedMetadata = Map(
        "7" -> new RuntimeException("Test"),
        "11212" -> new RuntimeException("Test")
      )
      val stateStore = mock[StateStore]
      val sessionStore = new SessionStore(conf, stateStore)
      when(stateStore.getChildren(sessionPath))
        .thenReturn((validMetadata ++ corruptedMetadata).keys.toList)

      validMetadata.foreach { case (id, m) =>
        when(stateStore.get[TestRecoveryMetadata](s"$sessionPath/$id")).thenReturn(m)
      }

      corruptedMetadata.foreach { case (id, ex) =>
        when(stateStore.get[TestRecoveryMetadata](s"$sessionPath/$id")).thenThrow(ex)
      }

      val s = sessionStore.getAllSessions[TestRecoveryMetadata](sessionType)
      // Verify normal metadata are retrieved.
      s.filter(_.isSuccess) should contain theSameElementsAs
        validMetadata.values.filter(_.isDefined).map(m => Success(m.get))
      // Verify exceptions are wrapped as in Try and are returned.
      s.filter(_.isFailure) should have size corruptedMetadata.size
    }

    it("should not throw if the state store is empty") {
      val stateStore = mock[StateStore]
      val sessionStore = new SessionStore(conf, stateStore)
      when(stateStore.getChildren(sessionPath)).thenReturn(Seq.empty)

      val s = sessionStore.getAllSessions[TestRecoveryMetadata](sessionType)
      s.filter(_.isSuccess) shouldBe empty
    }

    it("should return correct next session id") {
      val stateStore = mock[StateStore]
      val sessionStore = new SessionStore(conf, stateStore)

      when(stateStore.get[SessionManagerState](sessionManagerPath)).thenReturn(None)
      sessionStore.getNextSessionId(sessionType) shouldBe 0

      val sms = SessionManagerState(100)
      when(stateStore.get[SessionManagerState](sessionManagerPath)).thenReturn(Some(sms))
      sessionStore.getNextSessionId(sessionType) shouldBe sms.nextSessionId
    }

    it("should remove session") {
      val stateStore = mock[StateStore]
      val sessionStore = new SessionStore(conf, stateStore)
      val id = 1

      sessionStore.remove(sessionType, 1)
      verify(stateStore).remove(s"$sessionPath/$id")
    }
  }
}
