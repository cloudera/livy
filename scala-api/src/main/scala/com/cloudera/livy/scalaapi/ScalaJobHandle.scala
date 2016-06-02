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

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future, TimeoutException}
import scala.util.Try

import com.cloudera.livy.JobHandle
import com.cloudera.livy.JobHandle.{Listener, State}

class ScalaJobHandle[T] private[livy] (jobHandle: JobHandle[T]) extends Future[T] {

  private var listener: JobHandle.Listener[T]  = null

  def getState(): State = jobHandle.getState

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    initiateListener(func, executor)
    if (isCompleted) {
      try {
        listener.onJobSucceeded(jobHandle, jobHandle.get())
      } catch {
        case e: Exception => listener.onJobFailed(jobHandle, e)
      }
    }
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
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    jobHandle.get()
  }

  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): ScalaJobHandle.this.type = {
    if (!atMost.isFinite()) {
      value
    } else jobHandle.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobHandle.wait(finishTime - time)
        }
      }
    }
    this
  }

  private def initiateListener[U](func: (Try[T]) => U, executor: ExecutionContext) = {
    listener = new Listener[T] {
      override def onJobQueued(job: JobHandle[T]): Unit = ???

      override def onJobCancelled(job: JobHandle[T]): Unit = ???

      override def onJobSucceeded(job: JobHandle[T], result: T): Unit = {
        val onCompleteTask = new Runnable {
          override def run(): Unit = {
            func(Try(result))
          }
        }
        executor.execute(onCompleteTask)
      }

      override def onJobStarted(job: JobHandle[T]): Unit = ???

      override def onJobFailed(job: JobHandle[T], cause: Throwable): Unit = {
        val onCompleteTask = new Runnable {
          override def run(): Unit = {
            func(Try(job.get()))
          }
        }
        executor.execute(onCompleteTask)
      }
    }
  }
}
