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

import java.util.Random
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object ScalaClientTestUtils extends FunSuite {

  val Timeout = 40

  def helloJob(context: ScalaJobContext): String = "hello"

  def throwExceptionJob(context: ScalaJobContext): Unit = throw new CustomTestFailureException

  def simpleSparkJob(context: ScalaJobContext): Long = {
    val r = new Random
    val count = 5
    val partitions = Math.min(r.nextInt(10) + 1, count)
    val buffer = new ArrayBuffer[Int]()
    for (a <- 1 to count) {
      buffer += r.nextInt()
    }
    context.sc.parallelize(buffer, partitions).count()
  }

  def assertAwait(lock: CountDownLatch): Unit = {
    assert(lock.await(Timeout, TimeUnit.SECONDS) == true)
  }

  def assertTestPassed[T](future: Future[T], expectedValue: T): Unit = {
    val result = Await.result(future, Timeout second)
    assert(result === expectedValue)
  }
}
