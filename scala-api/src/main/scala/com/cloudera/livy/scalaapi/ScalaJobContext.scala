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

class ScalaJobContext private[livy] (context: JobContext) {

  def sc: SparkContext = context.sc().sc

  def sqlctx: SQLContext =  context.sqlctx()

  def hivectx: HiveContext = context.hivectx()

  def streamingctx: StreamingContext = context.streamingctx().ssc

  def createStreamingContext(batchDuration: Long) = context.createStreamingContext(batchDuration)

  def stopStreamingContext() = context.stopStreamingCtx()

  def localTmpDir: File = context.getLocalTmpDir

}
