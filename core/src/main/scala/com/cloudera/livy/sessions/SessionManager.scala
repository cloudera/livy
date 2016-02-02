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

import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import com.cloudera.livy.{LivyConf, Logging}

object SessionManager {
  val SESSION_TIMEOUT = LivyConf.Entry("livy.server.session.timeout", "1h")
}

class SessionManager[S <: Session, R](val livyConf: LivyConf, factory: SessionFactory[S, R])
  extends Logging {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  private[this] final val _idCounter = new AtomicInteger()
  private[this] final val _sessions = mutable.Map[Int, S]()

  private[this] final val sessionTimeout =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(SessionManager.SESSION_TIMEOUT))

  val livyHome = livyConf.livyHome().getOrElse {
    val isTest = sys.env.get("livy.test").map(_ == "true").isDefined
    if (isTest) {
       Files.createTempDirectory("livyTemp").toUri.toString
    } else {
      throw new RuntimeException("livy.home must be specified!")
    }
  }

  factory.setLivyHome(livyHome)

  logger.info(s"Live Home = $livyHome")

  private[this] final val garbageCollector = new GarbageCollector

  garbageCollector.setDaemon(true)
  garbageCollector.start()

  def create(createRequest: R): S = {
    val id = _idCounter.getAndIncrement
    val session: S = factory.create(id, createRequest)

    info("created session %s" format session.id)

    synchronized {
      _sessions.put(session.id, session)
      session
    }
  }

  def get(id: Int): Option[S] = _sessions.get(id)

  def size(): Int = _sessions.size

  def all(): Iterable[S] = _sessions.values

  def delete(id: Int): Option[Future[Unit]] = {
    get(id).map(delete)
  }

  def delete(session: S): Future[Unit] = {
    session.stop().map { case _ =>
      synchronized {
        _sessions.remove(session.id)
      }

      Unit
    }
  }

  def shutdown(): Unit = {}

  def collectGarbage(): Future[Iterable[Unit]] = {
    def expired(session: Session): Boolean = {
      session.lastActivity.orElse(session.stoppedTime) match {
        case Some(lastActivity) =>
          val currentTime = System.nanoTime()
          currentTime - lastActivity > math.max(sessionTimeout, session.timeout)
        case None =>
          false
      }
    }

    Future.sequence(all().filter(expired).map(delete))
  }

  private class GarbageCollector extends Thread("session gc thread") {

    private var finished = false

    override def run(): Unit = {
      while (!finished) {
        collectGarbage()
        Thread.sleep(60 * 1000)
      }
    }

    def shutdown(): Unit = {
      finished = true
    }
  }
}
