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

import scala.concurrent.{CanAwait, ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.Duration
import scala.util.Try

import com.cloudera.livy.JobHandle
import com.cloudera.livy.JobHandle.{Listener, State}

class ScalaJobHandle[T] private[livy] (jobHandle: JobHandle[T]) extends Future[T] {

  def state: State = jobHandle.getState()

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    jobHandle.addListener(new AbstractScalaJobHandleListener[T] {
      override def onJobSucceeded(job: JobHandle[T], result: T): Unit = {
        val onJobSucceededTask = new Runnable {
          override def run(): Unit = func(Try(result))
        }
        executor.execute(onJobSucceededTask)
      }

      override def onJobFailed(job: JobHandle[T], cause: Throwable): Unit = {
        val onJobFailedTask = new Runnable {
          override def run(): Unit = func(Try(getJavaFutureResult(job)))
        }
        executor.execute(onJobFailedTask)
      }
    })
  }

  def onJobQueued[U](func: => Unit)(implicit executor: ExecutionContext): Unit = {
    jobHandle.addListener(new AbstractScalaJobHandleListener[T] {
      override def onJobQueued(job: JobHandle[T]): Unit = {
        val onJobQueuedTask = new Runnable {
          override def run(): Unit = func
        }
        executor.execute(onJobQueuedTask)
      }
    })
  }

  def onJobStarted[U](func: => Unit)(implicit executor: ExecutionContext): Unit = {
    jobHandle.addListener(new AbstractScalaJobHandleListener[T] {
      override def onJobStarted(job: JobHandle[T]): Unit = {
        val onJobStartedTask = new Runnable {
          override def run(): Unit = func
        }
        executor.execute(onJobStartedTask)
      }
    })
  }

  def onJobCancelled[U](func: Boolean => Unit)(implicit executor: ExecutionContext): Unit = {
    jobHandle.addListener(new AbstractScalaJobHandleListener[T] {
      override def onJobCancelled(job: JobHandle[T]): Unit = {
        val onJobCancelledTask = new Runnable {
          override def run(): Unit = func(job.cancel(false))
        }
        executor.execute(onJobCancelledTask)
      }
    })
  }

  override def isCompleted: Boolean = jobHandle.isDone

  override def value: Option[Try[T]] = {
    if (isCompleted) {
      Some(Try(getJavaFutureResult(jobHandle)))
    } else {
      None
    }
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T =
    getJavaFutureResult(jobHandle, atMost)

  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): ScalaJobHandle.this.type = {
    getJavaFutureResult(jobHandle, atMost)
    this
  }
}

private abstract class AbstractScalaJobHandleListener[T] extends Listener[T] {
  override def onJobQueued(job: JobHandle[T]): Unit = {}

  override def onJobCancelled(job: JobHandle[T]): Unit = {}

  override def onJobSucceeded(job: JobHandle[T], result: T): Unit = {}

  override def onJobStarted(job: JobHandle[T]): Unit = {}

  override def onJobFailed(job: JobHandle[T], cause: Throwable): Unit = {}
}

