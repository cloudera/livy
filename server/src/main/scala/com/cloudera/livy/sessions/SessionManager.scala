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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import com.cloudera.livy.server.interactive.{InteractiveRecoveryMetadata, InteractiveSession}
import com.cloudera.livy.server.recovery.{SessionManagerListener, SessionStore}
import com.cloudera.livy.sessions.Session.RecoveryMetadata

object SessionManager {
  val SESSION_RECOVERY_MODE_OFF = "off"
  val SESSION_RECOVERY_MODE_RECOVERY = "recovery"
  val SESSION_TIMEOUT = LivyConf.Entry("livy.server.session.timeout", "1h")
}

class BatchSessionManager(
    livyConf: LivyConf,
    sessionStore: SessionStore,
    mockSessions: Option[Seq[BatchSession]] = None)
  extends SessionManager[BatchSession, BatchRecoveryMetadata] (
    livyConf, BatchSession.recover(_, livyConf, sessionStore), sessionStore, "batch", mockSessions)

class InteractiveSessionManager(
  livyConf: LivyConf,
  sessionStore: SessionStore,
  mockSessions: Option[Seq[InteractiveSession]] = None)
  extends SessionManager[InteractiveSession, InteractiveRecoveryMetadata] (
    livyConf,
    InteractiveSession.recover(_, livyConf, sessionStore),
    sessionStore,
    "interactive",
    mockSessions)

class SessionManager[S <: Session, R <: RecoveryMetadata : ClassTag](
    livyConf: LivyConf,
    sessionRecovery: R => S,
    sessionStore: SessionStore,
    sessionType: String,
    mockSessions: Option[Seq[S]] = None)
  extends SessionManagerListener with Logging {

  import SessionManager._

  protected implicit def executor: ExecutionContext = ExecutionContext.global

  protected[this] final val idCounter = new AtomicInteger(0)
  protected[this] final val sessions = mutable.LinkedHashMap[Int, S]()

  private[this] final val sessionTimeout =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(SessionManager.SESSION_TIMEOUT))

  mockSessions.getOrElse(recover()).foreach(register)
  new GarbageCollector().start()

  def nextId(): Int = synchronized {
    val id = idCounter.getAndIncrement()
    sessionStore.saveNextSessionId(sessionType, idCounter.get())
    id
  }

  def register(session: S): S = {
    info(s"Registering new session ${session.id}")
    synchronized {
      sessions.put(session.id, session)
    }
    session
  }

  def get(id: Int): Option[S] = sessions.get(id)

  def size(): Int = sessions.size

  def all(): Iterable[S] = sessions.values

  def delete(id: Int): Option[Future[Unit]] = {
    get(id).map(delete)
  }

  def delete(session: S): Future[Unit] = {
    session.stop().map { case _ =>
      try {
        sessionStore.remove(sessionType, session.id)
        synchronized {
          sessions.remove(session.id)
        }
      } catch {
        case NonFatal(e) =>
          error("Exception was thrown during stop session:", e)
          throw e
      }
    }
  }

  def shutdown(): Unit = {
    val recoveryEnabled = livyConf.get(LivyConf.RECOVERY_MODE) != SESSION_RECOVERY_MODE_OFF
    if (!recoveryEnabled) {
      sessions.values.map(_.stop).foreach { future =>
        Await.ready(future, Duration.Inf)
      }
    }
  }

  def collectGarbage(): Future[Iterable[Unit]] = {
    def expired(session: Session): Boolean = {
      val currentTime = System.nanoTime()
      currentTime - session.lastActivity > math.max(sessionTimeout, session.timeout)
    }

    Future.sequence(all().filter(expired).map(delete))
  }

  private def recover(): Seq[S] = {
    // Recover next session id from state store and create SessionManager.
    idCounter.set(sessionStore.getNextSessionId(sessionType))

    // Retrieve session recovery metadata from state store.
    val sessionMetadata = sessionStore.getAllSessions[R](sessionType)

    // Recover session from session recovery metadata.
    val recoveredSessions = sessionMetadata.flatMap(_.toOption).map(sessionRecovery)

    info(s"Recovered ${recoveredSessions.length} $sessionType sessions." +
      s" Next session id: $idCounter")

    // Print recovery error.
    val recoveryFailure = sessionMetadata.filter(_.isFailure).map(_.failed.get)
    recoveryFailure.foreach(ex => error(ex.getMessage, ex.getCause))

    recoveredSessions
  }

  private class GarbageCollector extends Thread("session gc thread") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        collectGarbage()
        Thread.sleep(60 * 1000)
      }
    }

  }

  override def register(recoveryData: BatchRecoveryMetadata): Unit = {
    synchronized {
      sessions.contains(recoveryData.id) match {
        case true =>
          // This session manager added the session. No need to register it
          logger.info(s"The session manager already has information for $recoveryData")
        case false =>
          logger.info(s"The session saw new session $recoveryData from ZooKeeper")
          recoveryData.appTag match {
            case null =>
              // the app is not registered in yarn yet. wait until it is
              logger.info(s"Session manager cannot find an appTag for $recoveryData from ZooKeeper")
            case appTag => BatchSession.update(recoveryData, livyConf, sessionStore, None)
              logger.info(s"Session Manager found new Batch session $recoveryData from ZooKeeper")
              val session = BatchSession.update(recoveryData, livyConf, sessionStore)
              register(session.asInstanceOf[S])
          }
      }
    }
  }


  override def remove(recoveryData: BatchRecoveryMetadata): Unit = {
    synchronized {
      sessions.contains(recoveryData.id) match {
        case true =>
          // This session manager added the session. No need to register it
          logger.info(s"Removing session $recoveryData")
          sessions.remove(recoveryData.id)
        case false =>
          logger.info(s"The session is not present")
      }
    }
  }

}
