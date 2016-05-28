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

import java.util.concurrent.{Future, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.Promise
import scala.util.Try

class PollingContainer[T](executor: ScheduledExecutorService, jFuture: Future[T]) {

  private val initialDelay = 1
  private val longDelay = 1
  private var scheduledFuture: ScheduledFuture[_] = _
  val promise = Promise[T]

  def poll(): Unit = {
    scheduledFuture = executor.scheduleWithFixedDelay(new Poller(), initialDelay, longDelay, TimeUnit.SECONDS)
  }

  protected class Poller extends Runnable {
    override def run(): Unit =  {
      if(jFuture.isDone) {
        promise.complete(Try(jFuture.get()))
        scheduledFuture.cancel(false)
      }
    }
  }
}
