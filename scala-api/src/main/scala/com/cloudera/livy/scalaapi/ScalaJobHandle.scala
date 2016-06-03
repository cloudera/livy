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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future, TimeoutException}
import scala.util.Try

import com.cloudera.livy.JobHandle
import com.cloudera.livy.JobHandle.{Listener, State}

class ScalaJobHandle[T] private[livy] (jobHandle: JobHandle[T]) extends Future[T] {

  private val listener = initiateListener()
  private var callback: (Try[T]) => Any = null
  private implicit var executor: ExecutionContext = null

  def getState(): State = jobHandle.getState

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    callback = func
    this.executor = executor
    jobHandle.addListener(listener)
  }

  override def isCompleted: Boolean = jobHandle.isDone

  override def value: Option[Try[T]] = {
    if (isCompleted) {
      Some(Try(jobHandle.get()))
    } else {
      None
    }
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T =
    jobHandle.get(atMost.toMillis, TimeUnit.MILLISECONDS)

  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): ScalaJobHandle.this.type = {
    if (!atMost.isFinite()) {
      value
    } else jobHandle.synchronized {
      val finishTime = System.nanoTime() + atMost.toNanos
      while (!isCompleted) {
        val time = System.nanoTime()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobHandle.wait(((finishTime - time)/1000000))
        }
      }
    }
    this
  }

  private def initiateListener(): Listener[T] = {
    new Listener[T] {
      override def onJobQueued(job: JobHandle[T]): Unit = {}

      override def onJobCancelled(job: JobHandle[T]): Unit = {}

      override def onJobSucceeded(job: JobHandle[T], result: T): Unit = {
        val onCompleteTask = new Runnable {
          override def run(): Unit = {
            callback(Try(result))
          }
        }
        executor.execute(onCompleteTask)
      }

      override def onJobStarted(job: JobHandle[T]): Unit = {}

      override def onJobFailed(job: JobHandle[T], cause: Throwable): Unit = {
        val onCompleteTask = new Runnable {
          override def run(): Unit = {
            callback(Try(job.get()))
          }
        }
        executor.execute(onCompleteTask)
      }
    }
  }
}
