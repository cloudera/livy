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

import java.util.Date

import scala.concurrent.duration.{Deadline, Duration, FiniteDuration}

import com.cloudera.livy.sessions.Session.RecoveryMetadata
import com.cloudera.livy.LivyConf
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.{Session, SessionManager}

/**
  * A session trait to provide heartbeat expiration check.
  * Note: Session will not expire if heartbeat() was never called.
  */
trait SessionHeartbeat {
  protected val heartbeatTimeout: FiniteDuration

  private var _lastHeartbeat: Date = _ // For reporting purpose
  private var heartbeatDeadline: Option[Deadline] = None

  def heartbeat(): Unit = synchronized {
    if (heartbeatTimeout > Duration.Zero) {
      heartbeatDeadline = Some(heartbeatTimeout.fromNow)
    }

    _lastHeartbeat = new Date()
  }

  def lastHeartbeat: Date = synchronized { _lastHeartbeat }

  def heartbeatExpired: Boolean = synchronized { heartbeatDeadline.exists(_.isOverdue()) }
}

/**
  * Servlet can mixin this trait to update session's heartbeat
  * whenever a /sessions/:id REST call is made. e.g. GET /sessions/:id
  * Note: GET /sessions doesn't update heartbeats.
  */
trait SessionHeartbeatNotifier[S <: Session with SessionHeartbeat, R <: RecoveryMetadata]
  extends SessionServlet[S, R] {

  abstract override protected def withUnprotectedSession(fn: (S => Any)): Any = {
    super.withUnprotectedSession { s =>
      s.heartbeat()
      fn(s)
    }
  }

  abstract override protected def withViewAccessSession(fn: (S => Any)): Any = {
    super.withViewAccessSession { s =>
      s.heartbeat()
      fn(s)
    }
  }

  abstract override protected def withModifyAccessSession(fn: (S) => Any): Any = {
    super.withModifyAccessSession { s =>
      s.heartbeat()
      fn(s)
    }
  }
}

/**
  * A SessionManager trait.
  * It will create a thread that periodically deletes sessions with expired heartbeat.
  */
trait SessionHeartbeatWatchdog[S <: Session with SessionHeartbeat, R <: RecoveryMetadata] {
  self: SessionManager[S, R] =>

  private val watchdogThread = new Thread(s"HeartbeatWatchdog-${self.getClass.getName}") {
    override def run(): Unit = {
      val interval = livyConf.getTimeAsMs(LivyConf.HEARTBEAT_WATCHDOG_INTERVAL)
      info("Heartbeat watchdog thread started.")
      while (true) {
        deleteExpiredSessions()
        Thread.sleep(interval)
      }
    }
  }

  protected def start(): Unit = {
    assert(!watchdogThread.isAlive())

    watchdogThread.setDaemon(true)
    watchdogThread.start()
  }

  private[interactive] def deleteExpiredSessions(): Unit = {
    // Delete takes time. If we use .filter().foreach() here, the time difference between we check
    // expiration and the time we delete the session might be huge. To avoid that, check expiration
    // inside the foreach block.
    sessions.values.foreach { s =>
      if (s.heartbeatExpired) {
        info(s"Session ${s.id} expired. Last heartbeat is at ${s.lastHeartbeat}.")
        try { delete(s) } catch {
          case t: Throwable =>
            warn(s"Exception was thrown when deleting expired session ${s.id}", t)
        }
      }
    }
  }
}
