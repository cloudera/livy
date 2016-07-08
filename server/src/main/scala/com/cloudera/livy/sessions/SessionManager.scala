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

import com.cloudera.livy.{LivyConf, Logging}

object SessionManager {
  val SESSION_TIMEOUT = LivyConf.Entry("livy.server.session.timeout", "1h")
}

class SessionManager[S <: Session](val livyConf: LivyConf) extends Logging {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  private[this] final val idCounter = new AtomicInteger()
  private[this] final val sessions = mutable.LinkedHashMap[Int, S]()

  private[this] final val sessionTimeout =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(SessionManager.SESSION_TIMEOUT))

  new GarbageCollector().start()

  def nextId(): Int = idCounter.getAndIncrement()

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
      synchronized {
        sessions.remove(session.id)
      }
    }
  }

  def shutdown(): Unit = {
    // TODO: if recovery or HA is available, sessions should not be stopped.
    sessions.values.map(_.stop).foreach { future =>
      Await.ready(future, Duration.Inf)
    }
  }

  def collectGarbage(): Future[Iterable[Unit]] = {
    def expired(session: Session): Boolean = {
      val currentTime = System.nanoTime()
      currentTime - session.lastActivity > math.max(sessionTimeout, session.timeout)
    }

    Future.sequence(all().filter(expired).map(delete))
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

}
