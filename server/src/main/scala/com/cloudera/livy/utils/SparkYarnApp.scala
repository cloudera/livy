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

import java.util.concurrent.TimeoutException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils

import com.cloudera.livy.{LivyConf, Logging, Utils}
import com.cloudera.livy.util.LineBufferedProcess

object SparkYarnApp extends Logging {
  // YarnClient is thread safe. Create once, share it across threads.
  lazy val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }

  private def getYarnTagToAppIdTimeout(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT) milliseconds

  private def getYarnPollInterval(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_POLL_INTERVAL) milliseconds
}

/**
 * Provide a class to control a Spark application using YARN API.
 *
 * @param appTag An app tag that can unique identify the YARN app.
 * @param appIdOption The appId of the YARN app. If this's None, SparkYarnApp will find it
 *                    using appTag.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApp.log() will include its log.
 * @param listener Optional listener for notification of appId discovery and app state changes.
 */
class SparkYarnApp private[utils] (
    appTag: String,
    appIdOption: Option[String],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener],
    livyConf: LivyConf,
    yarnClient: => YarnClient = SparkYarnApp.yarnClient) // For unit test.
  extends SparkApp
  with Logging {
  import SparkYarnApp._

  private val appIdPromise: Promise[ApplicationId] = Promise()
  private[utils] var state: SparkApp.State = SparkApp.State.STARTING
  private var yarnDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]

  override def log(): IndexedSeq[String] =
    process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++ yarnDiagnostics

  override def kill(): Unit = synchronized {
    if (isRunning) {
      try {
        val timeout = SparkYarnApp.getYarnTagToAppIdTimeout(livyConf)
        yarnClient.killApplication(Await.result(appIdPromise.future, timeout))
      } catch {
        // We cannot kill the YARN app without the app id.
        // There's a chance the YARN app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException =>
          warn("Deleting a session while its YARN application is not found.")
          yarnAppMonitorThread.interrupt()
      } finally {
        process.foreach(_.destroy())
      }
    }
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

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
      pollInterval: Duration,
      deadline: Deadline): ApplicationId = {
    val appTagLowerCase = appTag.toLowerCase()

    // FIXME Should not loop thru all YARN applications but YarnClient doesn't offer an API.
    // Consider calling rmClient in YarnClient directly.
    val appType = Set("SPARK").asJava
    yarnClient.getApplications(appType).asScala.find(_.getApplicationTags.contains(appTagLowerCase))
    match {
      case Some(app) => app.getApplicationId
      case None =>
        if (deadline.isOverdue) {
          throw new Exception(s"No YARN application is tagged with $appTagLowerCase.")
        } else {
          Clock.sleep(pollInterval.toMillis)
          getAppIdFromTag(appTagLowerCase, pollInterval, deadline)
        }
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

  // Exposed for unit test.
  private[utils] def mapYarnState(
      appId: ApplicationId,
      yarnAppState: YarnApplicationState,
      finalAppStatus: FinalApplicationStatus): SparkApp.State.Value = {
    yarnAppState match {
      case (YarnApplicationState.NEW |
            YarnApplicationState.NEW_SAVING |
            YarnApplicationState.SUBMITTED |
            YarnApplicationState.ACCEPTED) => SparkApp.State.STARTING
      case YarnApplicationState.RUNNING => SparkApp.State.RUNNING
      case YarnApplicationState.FINISHED =>
        finalAppStatus match {
          case FinalApplicationStatus.SUCCEEDED => SparkApp.State.FINISHED
          case FinalApplicationStatus.FAILED => SparkApp.State.FAILED
          case FinalApplicationStatus.KILLED => SparkApp.State.KILLED
          case s =>
            error(s"Unknown YARN final status $appId $s")
            SparkApp.State.FAILED
        }
      case YarnApplicationState.FAILED => SparkApp.State.FAILED
      case YarnApplicationState.KILLED => SparkApp.State.KILLED
    }
  }

  // Exposed for unit test.
  // TODO Instead of spawning a thread for every session, create a centralized thread and
  // batch YARN queries.
  private[utils] val yarnAppMonitorThread = Utils.startDaemonThread(s"yarnAppMonitorThread-$this") {
    try {
      // Wait for spark-submit to finish submitting the app to YARN.
      process.foreach { p =>
        val exitCode = p.waitFor()
        if (exitCode != 0) {
          throw new Exception(s"spark-submit exited with code $exitCode}.\n" +
            s"${process.get.inputLines.mkString("\n")}")
        }
      }

      // If appId is not known, query YARN by appTag to get it.
      val appId = try {
        appIdOption.map(ConverterUtils.toApplicationId).getOrElse {
          val pollInterval = getYarnPollInterval(livyConf)
          val deadline = getYarnTagToAppIdTimeout(livyConf).fromNow
          getAppIdFromTag(appTag, pollInterval, deadline)
        }
      } catch {
        case e: Exception =>
          appIdPromise.failure(e)
          throw e
      }
      appIdPromise.success(appId)

      Thread.currentThread().setName(s"yarnAppMonitorThread-$appId")
      listener.foreach(_.appIdKnown(appId.toString))

      val pollInterval = SparkYarnApp.getYarnPollInterval(livyConf)
      var appInfo = AppInfo()
      while (isRunning) {
        try {
          Clock.sleep(pollInterval.toMillis)

          // Refresh application state
          val appReport = yarnClient.getApplicationReport(appId)
          yarnDiagnostics = getYarnDiagnostics(appReport)
          changeState(mapYarnState(
            appReport.getApplicationId,
            appReport.getYarnApplicationState,
            appReport.getFinalApplicationStatus))

          val latestAppInfo = {
            val attempt =
              yarnClient.getApplicationAttemptReport(appReport.getCurrentApplicationAttemptId)
            val driverLogUrl =
              Try(yarnClient.getContainerReport(attempt.getAMContainerId).getLogUrl)
                .toOption
            AppInfo(driverLogUrl, Option(appReport.getTrackingUrl))
          }

          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
        } catch {
          // This exception might be thrown during app is starting up. It's transient.
          case e: ApplicationAttemptNotFoundException =>
          // Workaround YARN-4411: No enum constant FINAL_SAVING from getApplicationAttemptReport()
          case e: IllegalArgumentException =>
            if (e.getMessage.contains("FINAL_SAVING")) {
              debug("Encountered YARN-4411.")
            } else {
              throw e
            }
        }
      }

      debug(s"$appId $state ${yarnDiagnostics.mkString(" ")}")
    } catch {
      case e: InterruptedException =>
        yarnDiagnostics = ArrayBuffer("Session stopped by user.")
        changeState(SparkApp.State.KILLED)
      case e: Throwable =>
        error(s"Error whiling refreshing YARN state: $e")
        yarnDiagnostics = ArrayBuffer(e.toString, e.getStackTrace().mkString(" "))
        changeState(SparkApp.State.FAILED)
    }
  }
}
