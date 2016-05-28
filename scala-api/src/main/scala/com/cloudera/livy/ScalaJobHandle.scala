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

import com.cloudera.livy.JobHandle.State

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future, TimeoutException}
import scala.util.Try

class ScalaJobHandle[T](jobHandle: JobHandle[T]) extends Future[T] {

  private var listener: (Try[T] => Any)  = null

  def getState(): State = jobHandle.getState

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    listener = func
    if (isCompleted) {
      listener(Try(jobHandle.get()))
    }
  }

  override def isCompleted: Boolean = jobHandle.isDone

  override def value: Option[Try[T]] = Option(Try(jobHandle.get()))

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = jobHandle.get()

  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): ScalaJobHandle.this.type = ???
}
