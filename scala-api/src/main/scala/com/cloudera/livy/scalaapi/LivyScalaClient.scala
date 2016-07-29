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

/**
 * A client for submitting Spark-based jobs to a Livy backend.
 * @constructor  Creates a Scala client
 * @param  livyJavaClient  the java client of livy
 */
class LivyScalaClient(livyJavaClient: LivyClient) {

  private val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, "LivyScalaClient-PollingContainer")
      thread.setDaemon(true)
      thread
    }
  })

  /**
   * Submits a job for asynchronous execution.
   *
   * @param fn The job to be executed. It is a function that takes in a ScalaJobContext and
   * returns the result of the execution of the job with that context
   * @return A handle that can be used to monitor the job.
   */
  def submit[T](fn: ScalaJobContext => T): ScalaJobHandle[T] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = fn(new ScalaJobContext(jobContext))
    }
    new ScalaJobHandle(livyJavaClient.submit(job))
  }

  /**
   * Asks the remote context to run a job immediately.
   *
   * Normally, the remote context will queue jobs and execute them based on how many worker
   * threads have been configured. This method will run the submitted job in the same thread
   * processing the RPC message, so that queueing does not apply.
   *
   * It's recommended that this method only be used to run code that finishes quickly. This
   * avoids interfering with the normal operation of the context.
   *
   * @param fn The job to be executed. It is a function that takes in a ScalaJobContext and
   * returns the result of the execution of the job with that context
   * @return A handle that can be used to monitor the job.
   */
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

  /**
   * Stops the remote context.
   *
   * Any pending jobs will be cancelled, and the remote context will be torn down.
   *
   * @param  shutdownContext  Whether to shutdown the underlying Spark context. If false, the
   *                          context will keep running and it's still possible to send commands
   *                          to it, if the backend being used supports it.
   */
  def stop(shutdownContext: Boolean): Unit = {
    executor.shutdown()
    livyJavaClient.stop(shutdownContext)
  }

  /**
   * Upload a jar to be added to the Spark application classpath
   *
   * @param jar The local file to be uploaded
   * @return A future that can be used to monitor this operation
   */
  def uploadJar(jar: File): Future[_] = new PollingContainer(livyJavaClient.uploadJar(jar)).poll()

  /**
   * Adds a jar file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * If the provided URI has no scheme, it's considered to be relative to the default file system
   * configured in the Livy server.
   *
   * @param uri The location of the jar file.
   * @return A future that can be used to monitor the operation.
   */
  def addJar(uri: URI): Future[_] = new PollingContainer(livyJavaClient.addJar(uri)).poll()

  /**
   * Upload a file to be passed to the Spark application
   *
   * @param file The local file to be uploaded
   * @return A future that can be used to monitor this operation
   */
  def uploadFile(file: File): Future[_] =
    new PollingContainer(livyJavaClient.uploadFile(file)).poll()

  /**
   * Adds a file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * If the provided URI has no scheme, it's considered to be relative to the default file system
   * configured in the Livy server.
   *
   * @param uri The location of the file.
   * @return A future that can be used to monitor the operation.
   */
  def addFile(uri: URI): Future[_] = new PollingContainer(livyJavaClient.addFile(uri)).poll()

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
