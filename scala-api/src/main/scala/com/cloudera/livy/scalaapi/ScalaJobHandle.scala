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

/**
 *  A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 *
 *  @constructor Creates a ScalaJobHandle
 *  @param jobHandle the java JobHandle of livy
 *
 *  @define multipleCallbacks
 *  Multiple callbacks may be registered; there is no guarantee that they will be
 *  executed in a particular order.
 *
 *  @define nonDeterministic
 *  Note: using this method yields nondeterministic dataflow programs.
 *
 *  @define callbackInContext
 *  The provided callback always runs in the provided implicit
 *` ExecutionContext`, though there is no guarantee that the
 *  `execute()` method on the `ExecutionContext` will be called once
 *  per callback or that `execute()` will be called in the current
 *  thread. That is, the implementation may run multiple callbacks
 *  in a batch within a single `execute()` and it may run
 *  `execute()` either immediately or asynchronously.
 */
class ScalaJobHandle[T] private[livy] (jobHandle: JobHandle[T]) extends Future[T] {

  /**
   * Return the current state of the job.
   */
  def state: State = jobHandle.getState()

  /**
   *  When the job is completed, either through an exception, or a value,
   *  apply the provided function.
   *
   *  If the job has already been completed,
   *  this will either be applied immediately or be scheduled asynchronously.
   *
   *  $multipleCallbacks
   *  $callbackInContext
   */
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

  /** When this job is queued, apply the provided function.
   *
   *  $multipleCallbacks
   *  $callbackInContext
   */
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

  /** When this job has started, apply the provided function.
   *
   *  $multipleCallbacks
   *  $callbackInContext
   */
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

  /** When this job is cancelled, apply the provided function.
   *
   *  $multipleCallbacks
   *  $callbackInContext
   */
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

  /** Returns whether the job has already been completed with
   *  a value or an exception.
   *
   *  $nonDeterministic
   *
   *  @return    `true` if the job is already completed, `false` otherwise
   */
  override def isCompleted: Boolean = jobHandle.isDone

  /** The value of the job
   *
   *  If the job is not completed the returned value will be `None`.
   *  If the job is completed the value will be `Some(Success(t))`
   *  if it contains a valid result, or `Some(Failure(error))` if it contains
   *  an exception.
   */
  override def value: Option[Try[T]] = {
    if (isCompleted) {
      Some(Try(getJavaFutureResult(jobHandle)))
    } else {
      None
    }
  }

  /**
   * Await the completion of the job and return the result (of type `T`)
   *
   * Although this method is blocking, the internal use of [[scala.concurrent.blocking blocking]]
   * ensures that the underlying [[ExecutionContext]] to properly detect blocking and ensure
   * that there are no deadlocks.
   * @param  atMost
   *         maximum wait time, which may be negative (no waiting is done),
   *         [[scala.concurrent.duration.Duration.Inf Duration.Inf]] for unbounded waiting,
   *         or a finite positive duration
   * @return the result value if job is completed within the specific maximum wait time
   * @throws Exception     the underlying exception on the execution of the job
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T =
    getJavaFutureResult(jobHandle, atMost)

  /**
   * Await the completion of the job
   *
   * Although this method is blocking, the internal use of [[scala.concurrent.blocking blocking]]
   * ensures that the underlying [[ExecutionContext]] is prepared to properly manage the blocking.
   * @param  atMost
   *         maximum wait time, which may be negative (no waiting is done),
   *         [[scala.concurrent.duration.Duration.Inf Duration.Inf]] for unbounded waiting,
   *         or a finite positive duration
   * @return ScalaJobHandle
   * @throws InterruptedException     if the current thread is interrupted while waiting
   * @throws TimeoutException         if after waiting for the specified time the job
   *                                  is still not ready
   */
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
