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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.interactive.InteractiveSession

object SessionManager {
  val SESSION_TIMEOUT = LivyConf.Entry("livy.server.session.timeout", "1h")
}

class SessionManager[S <: Session](val livyConf: LivyConf) extends Logging {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  private[this] final val idCounter = new AtomicInteger()
  private[this] final val sessions = mutable.LinkedHashMap[Int, S]()

  private[this] final val sessionTimeout =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(SessionManager.SESSION_TIMEOUT))

  private val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(new Configuration())
  yarnClient.start()

  new GarbageCollector().start()
  new SessionAppStateMonitor().start()

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

  def checkAppState(): Unit = {
    val appIds = all().filter(s => s.isInstanceOf[InteractiveSession]
      && s.appId.isDefined
      && s.state.isActive)
      .foreach(s => {
        try {
          val appId = ConverterUtils.toApplicationId(s.appId.get)
          val appReport = yarnClient.getApplicationReport(appId)
          val appFinishedStates = Set(YarnApplicationState.FAILED, YarnApplicationState.KILLED,
            YarnApplicationState.FINISHED)
          if (appFinishedStates.contains(appReport.getYarnApplicationState)
            && s.state.isActive) {
            info(s"Stopping session ${s.id}, as the yarn app ${s.appId} is in " +
              s"state ${appReport.getYarnApplicationState}")
            s.stop()
          }
        } catch {
          case e: NumberFormatException => // ignore non-yarn apps
        }
      })
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

  private class SessionAppStateMonitor extends Thread("session app state monitor thread") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        checkAppState()
        Thread.sleep(60 * 1000)
      }
    }
  }
}
