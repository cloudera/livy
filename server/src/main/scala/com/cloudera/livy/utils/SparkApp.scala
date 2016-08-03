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

import com.cloudera.livy.LivyConf
import com.cloudera.livy.util.LineBufferedProcess

trait SparkAppListener {
  /** Fired when appId is known, even during recovery. */
  def appIdKnown(appId: String): Unit = {}

  /** Fired when the app state in the cluster changes. */
  def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {}
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
      val userYarnTags = sparkConf.get(uniqueAppTag).map("," + _).getOrElse("")
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
      process: LineBufferedProcess,
      livyConf: LivyConf,
      listener: Option[SparkAppListener]): SparkApp = {
    if (livyConf.isRunningOnYarn()) {
      SparkYarnApp.fromAppTag(uniqueAppTag, Some(process), listener, livyConf)
    } else {
      new SparkProcApp(process, listener)
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
