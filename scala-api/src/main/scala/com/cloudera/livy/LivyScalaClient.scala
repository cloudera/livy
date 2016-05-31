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

class LivyScalaClient(livyJavaClient: LivyClient, threadPoolSize: Integer) {

  private val executor = new ScheduledThreadPoolExecutor(threadPoolSize)

  def this(livyJavaClient: LivyClient) {
    this(livyJavaClient, 2)
  }

  def submit[T](block: ScalaJobContext => T): ScalaJobHandle[T] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = block(new ScalaJobContext(jobContext))
    }
    new ScalaJobHandle(livyJavaClient.submit(job))
  }

  def run[T](block: ScalaJobContext => T): Future[_] = {
    val job = new Job[T] {
      @throws(classOf[Exception])
      override def call(jobContext: JobContext): T = block(new ScalaJobContext(jobContext))
    }
    Future {
      new PollingContainer(executor, livyJavaClient.run(job)).poll()
    }
  }

  def stop(shutdownContext: Boolean) = livyJavaClient.stop(shutdownContext)

  def uploadJar(jar: File): Future[_] = Future {
    new PollingContainer(executor, livyJavaClient.uploadJar(jar)).poll()
  }

  def addJar(uRI: URI): Future[_] = Future {
    new PollingContainer(executor, livyJavaClient.addJar(uRI)).poll()
  }
  def uploadFile(file: File): Future[_] = Future {
    new PollingContainer(executor, livyJavaClient.uploadFile(file)).poll()
  }

  def addFile(uRI: URI): Future[_] = Future {
    new PollingContainer(executor, livyJavaClient.addFile(uRI)).poll()
  }

  def shutdown() = executor.shutdown()
}

