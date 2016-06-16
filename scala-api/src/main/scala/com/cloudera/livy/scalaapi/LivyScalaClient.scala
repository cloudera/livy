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
import java.net.URI
import java.util.concurrent.{Executors, Future => JFuture, ScheduledFuture, ThreadFactory, TimeUnit}

import scala.concurrent._
import scala.util.Try

import com.cloudera.livy._

class LivyScalaClient(livyJavaClient: LivyClient) {

  private val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, "LivyScalaClient-PollingContainer")
      thread.setDaemon(true)
      thread
    }
  })

  def submit[T](fn: ScalaJobContext => T): ScalaJobHandle[T] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = fn(new ScalaJobContext(jobContext))
    }
    new ScalaJobHandle(livyJavaClient.submit(job))
  }

  def run[T](fn: ScalaJobContext => T): Future[T] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = {
        val scalaJobContext = new ScalaJobContext(jobContext)
        fn(scalaJobContext)
      }
    }
    new PollingContainer(livyJavaClient.run(job)).poll()
  }

  def stop(shutdownContext: Boolean): Unit = {
    executor.shutdown()
    livyJavaClient.stop(shutdownContext)
  }

  def uploadJar(jar: File): Future[_] = new PollingContainer(livyJavaClient.uploadJar(jar)).poll()

  def addJar(uRI: URI): Future[_] = new PollingContainer(livyJavaClient.addJar(uRI)).poll()

  def uploadFile(file: File): Future[_] =
    new PollingContainer(livyJavaClient.uploadFile(file)).poll()

  def addFile(uRI: URI): Future[_] = new PollingContainer(livyJavaClient.addFile(uRI)).poll()

  private class PollingContainer[T] private[livy] (jFuture: JFuture[T]) extends Runnable {

    private val initialDelay = 1
    private val longDelay = 1
    private var scheduledFuture: ScheduledFuture[_] = _
    private val promise = Promise[T]

    def poll(): Future[T] = {
      scheduledFuture =
        executor.scheduleWithFixedDelay(this, initialDelay, longDelay, TimeUnit.SECONDS)
      promise.future
    }

    override def run(): Unit = {
      if (jFuture.isDone) {
        promise.complete(Try(getJavaFutureResult(jFuture)))
        scheduledFuture.cancel(false)
      }
    }
  }
}

