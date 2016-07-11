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
package com.cloudera.livy.scalaapi

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext

import com.cloudera.livy.JobContext

/**
 *  Holds runtime information about the job execution context.
 *
 *  An instance of this class is kept on the node hosting a remote Spark context and is made
 *  available to jobs being executed via RemoteSparkContext#submit().
 *
 *  Wrapper over the java JobContext of livy
 *
 *  @constructor Creates a ScalaJobContext
 *  @param context the java JobContext of livy
 */
class ScalaJobContext private[livy] (context: JobContext) {

  /** The shared SparkContext instance. */
  def sc: SparkContext = context.sc().sc

  /** The shared SQLContext inststance. */
  def sqlctx: SQLContext = context.sqlctx()

  /** The shared HiveContext inststance. */
  def hivectx: HiveContext = context.hivectx()

  /** Returns the StreamingContext which has already been created. */
  def streamingctx: StreamingContext = context.streamingctx().ssc

  /**
   * Creates the SparkStreaming context.
   *
   * @param batchDuration Time interval at which streaming data will be divided into batches,
   *                      in milliseconds.
   */
  def createStreamingContext(batchDuration: Long): Unit =
    context.createStreamingContext(batchDuration)

  /** Stops the SparkStreaming context. */
  def stopStreamingContext(): Unit = context.stopStreamingCtx()

  /**
   * Returns a local tmp dir specific to the context
   */
  def localTmpDir: File = context.getLocalTmpDir

}

