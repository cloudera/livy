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
package com.cloudera.livy.utils

import java.util.concurrent.{ThreadFactory, TimeoutException}
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{blocking, Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import com.cloudera.livy.Logging
import com.cloudera.livy.util.LineBufferedProcess

object SparkYarnApp extends Logging {
  private class NonDaemonThreadFactory extends ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setDaemon(true)
      t
    }
  }

  // Create a non daemon thread pool to run getAppIdFromTagAsync().
  // getAppIdFromTagAsync() might take a while and should not use the global default thread pool.
  private implicit val ec = ExecutionContext.fromExecutor(
    Executors.newCachedThreadPool(new NonDaemonThreadFactory()))

  // It takes at least 5 seconds to see the newly submitted app.
  private val APP_TAG_TO_ID_TIMEOUT = 10 seconds
  private val KILL_TIMEOUT = APP_TAG_TO_ID_TIMEOUT
  private val POLL_INTERVAL = 1 second

  // YarnClient is thread safe. Create once, share it across threads.
  private[this] lazy val yarnClient = YarnClient.createYarnClient()
  private[this] var yarnClientCreated = false

  def getYarnClient(): YarnClient = synchronized {
    if (!yarnClientCreated) {
      yarnClient.init(new YarnConfiguration())
      yarnClient.start()
      yarnClientCreated = true
    }
    yarnClient
  }

  def getAppIdFromTagAsync(appTag: String): Future[ApplicationId] =
    Future { getAppIdFromTag(appTag).get }

  /**
   * Find the corresponding YARN application id from an application tag.
   *
   * @param appTag The application tag tagged on the target application.
   *               If the tag is not unique, it returns the first application it found.
   *               It will be converted to lower case to match YARN's behaviour.
   * @return ApplicationId or the failure.
   */
  @tailrec
  private def getAppIdFromTag(
    appTag: String,
    deadline: Deadline = APP_TAG_TO_ID_TIMEOUT.fromNow): Try[ApplicationId] = {
    val appTagLowerCase = appTag.toLowerCase

    // FIXME Should not loop thru all YARN applications but YarnClient doesn't offer an API.
    // Consider calling rmClient in YarnClient directly.
    getYarnClient().getApplications().asScala.find(_.getApplicationTags.contains(appTagLowerCase))
      match {
        case Some(app) => Success(app.getApplicationId)
        case None =>
          if (deadline.isOverdue) {
            Failure(new Exception(s"No YARN application is tagged with $appTagLowerCase."))
          } else {
            blocking { Thread.sleep(POLL_INTERVAL.toMillis) }
            getAppIdFromTag(appTagLowerCase, deadline)
          }
      }
  }
}

/**
 * Provide a class to control a Spark application using YARN API.
 *
 * @param appIdFuture A future that returns the YARN application id for this application.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApp.log() will include its log.
 */
class SparkYarnApp (
    appIdFuture: Future[ApplicationId],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener])
  extends SparkApp
  with Logging {
  import SparkYarnApp.ec

  private lazy val yarnClient = SparkYarnApp.getYarnClient()
  private var state: SparkApp.State = SparkApp.State.STARTING
  private var yarnDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]

  override def log(): IndexedSeq[String] =
    (process map (_.inputLines) getOrElse ArrayBuffer.empty[String]) ++ yarnDiagnostics

  override def kill(): Unit = synchronized {
    if (isRunning) {
      process foreach (_.destroy())

      try {
        Await.result(appIdFuture map (yarnClient.killApplication), SparkYarnApp.KILL_TIMEOUT)
      } catch {
        // We cannot kill the YARN app without the app id.
        // There's a chance the YARN app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException =>
          warn("Deleting a session while its YARN application is not found.")
          yarnAppMonitorThread.interrupt()
      }
    }
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener foreach (_.stateChanged(state, newState))
      state = newState
    }
  }

  private def getYarnDiagnostics(appReport: ApplicationReport): IndexedSeq[String] = {
    Option(appReport.getDiagnostics)
      .filter(_.nonEmpty)
      .map[IndexedSeq[String]]("YARN Diagnostics:" +: _.split("\n"))
      .getOrElse(IndexedSeq.empty)
  }

  private def isRunning: Boolean = {
    state != SparkApp.State.FAILED && state != SparkApp.State.FINISHED &&
    state != SparkApp.State.KILLED
  }

  private def mapYarnState(applicationReport: ApplicationReport): SparkApp.State.Value = {
      applicationReport.getYarnApplicationState match {
      case (YarnApplicationState.NEW |
            YarnApplicationState.NEW_SAVING |
            YarnApplicationState.SUBMITTED |
            YarnApplicationState.ACCEPTED) => SparkApp.State.STARTING
      case YarnApplicationState.RUNNING => SparkApp.State.RUNNING
      case YarnApplicationState.FINISHED =>
        applicationReport.getFinalApplicationStatus match {
          case FinalApplicationStatus.SUCCEEDED => SparkApp.State.FINISHED
          case FinalApplicationStatus.FAILED => SparkApp.State.FAILED
          case FinalApplicationStatus.KILLED => SparkApp.State.KILLED
          case s =>
            error(s"Unknown YARN final status ${applicationReport.getApplicationId} $s")
            SparkApp.State.FAILED
        }
      case YarnApplicationState.FAILED => SparkApp.State.FAILED
      case YarnApplicationState.KILLED => SparkApp.State.KILLED
    }
  }

  // TODO Instead of spawning a thread for every session, create a centralized thread and
  // batch YARN queries.
  private val yarnAppMonitorThread = new Thread(s"yarnAppMonitorThread-$this") {
    override def run() = {
      @tailrec
      def waitForAppId(): ApplicationId = {
        try {
          // Wait for spark-submit to finish submitting the app to YARN.
          process map (_.waitFor()) match {
            case (None | Some(0)) => Await.result(appIdFuture, SparkYarnApp.POLL_INTERVAL)
            case Some(exitCode) =>
              throw new Exception(s"spark-submit exited with code $exitCode}.\n" +
                s"${process.get.inputLines mkString "\n"}")
          }
        } catch {
          case e: TimeoutException => waitForAppId()
        }
      }

      try {
        val appId = waitForAppId()

        Thread.currentThread().setName(s"yarnAppMonitorThread-$appId")

        listener foreach (_.appIdKnown(appId.toString))

        while (isRunning) {
          Thread.sleep(SparkYarnApp.POLL_INTERVAL.toMillis)

          // Refresh application state
          val appReport = yarnClient.getApplicationReport(appId)
          yarnDiagnostics = getYarnDiagnostics(appReport)
          changeState(mapYarnState(appReport))
        }

        debug(s"$appId $state ${yarnDiagnostics.mkString(" ")}")
      } catch {
        case e: InterruptedException =>
          yarnDiagnostics = ArrayBuffer("Session stopped by user.")
          changeState(SparkApp.State.KILLED)
        case e: Throwable =>
          error(s"Error whiling refreshing YARN state: $e")
          yarnDiagnostics = ArrayBuffer(e.toString)
          changeState(SparkApp.State.FAILED)
      }
    }
  }

  yarnAppMonitorThread.setDaemon(true)
  yarnAppMonitorThread.start()
}
