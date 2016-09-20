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

import scala.collection.JavaConverters._

import com.cloudera.livy.LivyConf
import com.cloudera.livy.util.LineBufferedProcess

object AppInfo {
  val DRIVER_LOG_URL_NAME = "driverLogUrl"
  val SPARK_UI_URL_NAME = "sparkUiUrl"
}

case class AppInfo(var driverLogUrl: Option[String] = None, var sparkUiUrl: Option[String] = None) {
  import AppInfo._
  def asJavaMap: java.util.Map[String, String] =
    Map(DRIVER_LOG_URL_NAME -> driverLogUrl.orNull, SPARK_UI_URL_NAME -> sparkUiUrl.orNull).asJava
}

trait SparkAppListener {
  /** Fired when appId is known, even during recovery. */
  def appIdKnown(appId: String): Unit = {}

  /** Fired when the app state in the cluster changes. */
  def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {}

  /** Fired when the app info is changed. */
  def infoChanged(appInfo: AppInfo): Unit = {}
}

/**
 * Provide factory methods for SparkApp.
 */
object SparkApp {
  private val SPARK_YARN_TAG_KEY = "spark.yarn.tags"

  object State extends Enumeration {
    val STARTING, RUNNING, FINISHED, FAILED, KILLED = Value
  }
  type State = State.Value

  /**
   * Return cluster manager dependent SparkConf.
   *
   * @param uniqueAppTag A tag that can uniquely identify the application.
   * @param livyConf
   * @param sparkConf
   */
  def prepareSparkConf(
      uniqueAppTag: String,
      livyConf: LivyConf,
      sparkConf: Map[String, String]): Map[String, String] = {
    if (livyConf.isRunningOnYarn()) {
      val userYarnTags = sparkConf.get(SPARK_YARN_TAG_KEY).map("," + _).getOrElse("")
      val mergedYarnTags = uniqueAppTag + userYarnTags
      sparkConf ++ Map(
        SPARK_YARN_TAG_KEY -> mergedYarnTags,
        "spark.yarn.submit.waitAppCompletion" -> "false")
    } else {
      sparkConf
    }
  }

  /**
   * Return a SparkApp object to control the underlying Spark application via YARN or spark-submit.
   *
   * @param uniqueAppTag A tag that can uniquely identify the application.
   */
  def create(
      uniqueAppTag: String,
      process: Option[LineBufferedProcess],
      livyConf: LivyConf,
      listener: Option[SparkAppListener]): SparkApp = {
    if (livyConf.isRunningOnYarn()) {
      SparkYarnApp.fromAppTag(uniqueAppTag, process, listener, livyConf)
    } else {
      require(process.isDefined, "process must not be None when Livy master is not YARN.")
      new SparkProcApp(process.get, listener)
    }
  }
}

/**
 * Encapsulate a Spark application.
 */
abstract class SparkApp {
  def kill(): Unit
  def log(): IndexedSeq[String]
}
