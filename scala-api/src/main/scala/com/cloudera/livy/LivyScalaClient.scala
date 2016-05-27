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

import java.io.File
import java.net.URI
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, Future => JFuture}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class LivyScalaClient(livyJavaClient: LivyClient) {

  private val threadPoolSize = 5
  private val initialDelay = 2
  private val longDelay = 2
  private val executor = new ScheduledThreadPoolExecutor(threadPoolSize)

  def submit[T](block: ScalaJobContext => T): JobHandle[T] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = block(client.wrapJobContext(jobContext))
    }
    livyJavaClient.submit(job)
  }

  def run[T](block: ScalaJobContext => T): Future[_] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = block(client.wrapJobContext(jobContext))
    }
    Future {
      doPoll(livyJavaClient.run(job))
    }
  }

  def stop(shutdownContext: Boolean) = livyJavaClient.stop(shutdownContext)

  def uploadJar(jar: File): Future[_] = Future {
    doPoll(livyJavaClient.uploadJar(jar))
  }

  def addJar(uRI: URI): Future[_] = Future {
    doPoll(livyJavaClient.addJar(uRI))
  }

  def uploadFile(file: File): Future[_] = Future {
    doPoll(livyJavaClient.uploadFile(file))
  }

  def addFile(uRI: URI): Future[_] = Future{
    doPoll(livyJavaClient.addFile(uRI))
  }

  private def doPoll[T](jFuture: JFuture[T]) = {
    val promise = Promise[T]
    val runnable = new Runnable {
       def run() = promise.complete(Try{jFuture.get()})
    }
    executor.scheduleWithFixedDelay(runnable , initialDelay, longDelay, TimeUnit.SECONDS)
  }

   def shutdown() = executor.shutdown()
}

