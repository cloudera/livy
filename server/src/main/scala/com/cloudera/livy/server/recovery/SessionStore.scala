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

import java.io.IOException

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

private[recovery] case class SessionManagerState(nextSessionId: Int)

/**
 * SessionStore provides high level functions to get/save session state from/to StateStore.
 */
class SessionStore(
    livyConf: LivyConf,
    store: => StateStore = StateStore.get) // For unit testing.
  extends Logging {

  private val STORE_VERSION: String = "v1"

  /**
   * Persist a session to the session state store.
   * @param m RecoveryMetadata for the session.
   */
  def save(sessionType: String, m: RecoveryMetadata): Unit = {
    store.set(sessionPath(sessionType, m.id), m)
  }

  /**
   * Return all sessions stored in the store with specified session type.
   */
  def getAllSessions[T <: RecoveryMetadata : ClassTag](sessionType: String): Seq[Try[T]] = {
    store.getChildren(sessionPath(sessionType))
      .flatMap { c => Try(c.toInt).toOption } // Ignore all non numerical keys
      .flatMap { id =>
        val p = sessionPath(sessionType, id)
        try {
          store.get[T](p).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(new IOException(s"Error getting session $p", e)))
        }
      }
  }

  /**
   * Return the next unused session id with specified session type.
   * It checks the SessionManagerState stored and returns the next free session id.
   * If no SessionManagerState is stored, it returns 0.
    * It saves the new session ID to the session store.
   *
   * @throws Exception If SessionManagerState stored is corrupted, it throws an error.
   */
  def getNextSessionId(sessionType: String): Int = synchronized {
    val nextSessionId = store.get[SessionManagerState](sessionManagerPath(sessionType))
      .map(_.nextSessionId).getOrElse(0)
    store.set(sessionManagerPath(sessionType), SessionManagerState(nextSessionId + 1))
    nextSessionId
  }

  /**
   * Remove a session from the state store.
   */
  def remove(sessionType: String, id: Int): Unit = {
    store.remove(sessionPath(sessionType, id))
  }

  private def sessionManagerPath(sessionType: String): String =
    s"$STORE_VERSION/$sessionType/state"

  private def sessionPath(sessionType: String): String =
    s"$STORE_VERSION/$sessionType"

  private def sessionPath(sessionType: String, id: Int): String =
    s"$STORE_VERSION/$sessionType/$id"
}
