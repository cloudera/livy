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

import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future, blocking}
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import com.cloudera.livy.Logging
import com.cloudera.livy.util.LineBufferedProcess

object SparkYarnApplication {
  private val YARN_GET_APP_ID_FROM_TAG_TIMEOUT = 5.minutes
  private val YARN_POLL_INTERVAL = 1.second

  // YarnClient is thread safe. Create once, share it across applications.
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

  /**
   * Find the corresponding YARN application id from an application tag.
   *
   * @param applicationTag The application tag tagged on the target application.
   *                       If the tag is not unique, it returns the first application it found.
   * @return ApplicationId or the failure.
   */
  @tailrec
  private def getApplicationIdFromTag(applicationTag: String,
                                      deadline: Deadline = YARN_GET_APP_ID_FROM_TAG_TIMEOUT.fromNow
                                     ): Try[ApplicationId] = {
    // FIXME Should not loop thru all YARN applications.
    getYarnClient().getApplications().find(_.getApplicationTags.contains(applicationTag)) match {
      case Some(app) => Success(app.getApplicationId)
      case None =>
        if (deadline.isOverdue) {
          Failure(new Exception(s"No YARN application is tagged with $applicationTag."))
        } else {
          blocking { Thread.sleep(YARN_POLL_INTERVAL.toMillis) }
          getApplicationIdFromTag(applicationTag, deadline)
        }
    }
  }

  // SparkYarnApplication uses a lot of blocking Future. It should have its own ExecutionContext.
  private implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
}

/**
 * Encapsulate a Spark application through YARN.
 * It provides state tracking & logging.
 *
 * @param applicationIdFuture A future that returns the YARN application id for this application.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApplication.log() will include its log.
 */
class SparkYarnApplication(applicationIdFuture: Future[ApplicationId],
                           process: Option[LineBufferedProcess] = None)
  extends SparkApplication with Logging {

  /**
   * Construct a Spark application from an application tag.
   *
   * @param applicationTag The application tag tagged on the target application.
   *                       If the tag is not unique, it picks the first application it found.
   * @param process The spark-submit process launched the YARN application. This is optional.
   *                If it's provided, SparkYarnApplication.log() will include its log.
   */
  def this(applicationTag: String, process: Option[LineBufferedProcess]) =
    this(Future { SparkYarnApplication.getApplicationIdFromTag(applicationTag).get }
    (SparkYarnApplication.ec), process)

  import SparkYarnApplication.ec

  private lazy val yarnClient = SparkYarnApplication.getYarnClient()
  private var state: YarnApplicationState = YarnApplicationState.NEW
  private var finalDiagnosticsLog: IndexedSeq[String] = ArrayBuffer.empty[String]

  private val pollThread = new Thread(s"yarnPollThread_$this") {
    override def run() = {
      val applicationId = Await.result(applicationIdFuture, Duration.Inf)

      while (isRunning) {
        // Refresh application state
        state = yarnClient.getApplicationReport(applicationId).getYarnApplicationState
        Thread.sleep(SparkYarnApplication.YARN_POLL_INTERVAL.toMillis)
      }

      // Log final YARN diagnostics for better error reporting.
      val finalDiagnostics = yarnClient.getApplicationReport(applicationId).getDiagnostics
      if (!finalDiagnostics.isEmpty) {
        finalDiagnosticsLog = finalDiagnostics.split("\n")
      }

      info(s"$applicationId $state $finalDiagnostics")
    }
  }

  pollThread.start()

  override def stop(): Unit = {
    if (isRunning) {
      Await.result(applicationIdFuture.map(yarnClient.killApplication), Duration.Inf)
    }
  }

  override def log(): IndexedSeq[String] = {
    process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++ finalDiagnosticsLog
  }

  override def waitFor(): Int = {
    pollThread.join()

    state match {
      case YarnApplicationState.FINISHED => 0
      case YarnApplicationState.FAILED | YarnApplicationState.KILLED => 1
      case _ =>
        error(s"Unexpected YARN state ${applicationIdFuture.value.get.get} $state")
        1
    }
  }

  private def isRunning: Boolean = {
    state != YarnApplicationState.FAILED &&
    state != YarnApplicationState.FINISHED &&
    state != YarnApplicationState.KILLED
  }
}
