/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.Promise
import scala.concurrent.Future

package object client {

  implicit class ScalaWrapper(livyJavaClient: LivyClient) {
    def asScalaClient = new LivyScalaClient(livyJavaClient)
  }

  def asScalaJobContext(context: JobContext): ScalaJobContext = {
    new ScalaJobContext {
      override def hivectx = context.hivectx()

      override def getLocalTmpDir = context.getLocalTmpDir

      override def createStreamingContext(batchDuration: Long) = context.createStreamingContext(batchDuration)

      override def streamingctx: StreamingContext = context.streamingctx().ssc

      override def stopStreamingContext: Unit = context.stopStreamingCtx()

      override def sc: SparkContext = context.sc().sc

      override def sqlctx: SQLContext = context.sqlctx()
    }
  }

  def asScalaFuture[T](jobHandle: JobHandle[T]): Future[T] = {
    val promise = Promise[T]
    jobHandle.addListener(new JobHandle.Listener[T] {
      override def onJobQueued(job: JobHandle[T]): Unit = ???

      override def onJobCancelled(job: JobHandle[T]): Unit = ???

      override def onJobSucceeded(job: JobHandle[T], result: T): Unit = promise.trySuccess(job.get())

      override def onJobStarted(job: JobHandle[T]): Unit = ???

      override def onJobFailed(job: JobHandle[T], cause: Throwable): Unit = promise.tryFailure(cause)
    })
    promise.future
  }
}


