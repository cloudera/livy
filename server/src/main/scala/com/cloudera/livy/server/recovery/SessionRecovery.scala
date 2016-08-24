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

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import com.cloudera.livy.sessions.{Session, SessionManager}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

object SessionRecoveryMode {
  val OFF = "off"
  val RECOVERY = "recovery"
}

class SessionRecovery(sessionStore: SessionStore, livyConf: LivyConf) extends Logging {
  def recoverBatchSessions(): SessionManager[BatchSession] = {
    recover[BatchSession, BatchRecoveryMetadata] (
      BatchSession.RECOVERY_SESSION_TYPE, classOf[BatchRecoveryMetadata]) {
        BatchSession.recover(_, livyConf, sessionStore)
      }
  }

  private[recovery] def recover[S <: Session, R <: RecoveryMetadata]
      (sessionType: String, metadataType: Class[R])
      (recoveryFun: (R => S))
    : SessionManager[S] = {
    // Recover next session id from state store and create SessionManager.
    val nextSessionId = sessionStore.getNextSessionId(sessionType)
    val sessionManager = new SessionManager[S](nextSessionId, livyConf)

    // Retrieve session recovery metadata from state store.
    val sessionMetadata = sessionStore.getAllSessions(sessionType, metadataType)

    // Call recoveryFun to recover session from session recovery metadata.
    val recoveredSessions = sessionMetadata.flatMap(_.toOption).map(recoveryFun)

    // Add recovered sessions to SessionManager.
    recoveredSessions.foreach(sessionManager.register)

    info(s"Recovered ${recoveredSessions.length} $sessionType sessions." +
      s" Next session id: $nextSessionId")

    // Print recovery error.
    val recoveryFailure = sessionMetadata.filter(_.isFailure).map(_.failed.get)
    recoveryFailure.foreach(ex => error(ex.getMessage, ex.getCause))

    sessionManager
  }
}
