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

import java.util.concurrent.Executors

import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import com.cloudera.livy.JobHandle

class ScalaJobHandleTest extends FunSuite with BeforeAndAfter with ScalaFutures {

  val jobHandle = mock(classOf[JobHandle[String]])
  val scalaJobHandle = new ScalaJobHandle(jobHandle)

  test("get result when job is already complete") {
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    val result  = Await.result(scalaJobHandle, 5 seconds)
    assert(result == "hello")
    verify(jobHandle, times(1)).get()
    verify(jobHandle, times(1)).isDone
  }

  test("get result when the thread waits for the mentioned duration for job to complete") {
    when(jobHandle.isDone).thenReturn(false).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    val result  = Await.result(scalaJobHandle, 5 seconds)
    assert(result == "hello")
    verify(jobHandle, times(1)).get()
    verify(jobHandle, times(2)).isDone
  }

  test("ready with Infinite Duration") {
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    val result  = Await.ready(scalaJobHandle, Duration.Undefined)
    assert(result == scalaJobHandle)
    verify(jobHandle, times(1)).get()
  }

  test("onCompleteSucess with global ExecutionContext") {
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    scalaJobHandle.onComplete(onCompleteSuccessCallbackFunc)
    verify(jobHandle, times(1)).isDone()
    verify(jobHandle, times(1)).get()
  }

  test("onCompleteSucess with user defined ExecutionContext") {
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    scalaJobHandle.onComplete(onCompleteSuccessCallbackFunc)(ec)
    verify(jobHandle, times(1)).isDone()
    verify(jobHandle, times(1)).get()
  }

  test("onCompleteFailure with global ExecutionContext") {
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenThrow(new CustomTestFailureException)
    scalaJobHandle.onComplete(onCompleteFailureCallbackFunc)
    verify(jobHandle, times(1)).isDone()
    verify(jobHandle, times(2)).get()
  }

  private def onCompleteSuccessCallbackFunc(func: Try[String]) = func match{
    case Success(t) => {
      assert(t === "hello")
    }
    case Failure(e) => {
      fail("Should not trigger Failure callback in onCompleteSuccessCallbackFunc")
    }
  }

  private def onCompleteFailureCallbackFunc(func: Try[String]) = func match{
    case Success(t) => {
      fail("Should not trigger Success callback onCompleteFailureCallbackFunc")
    }
    case Failure(e) => {
      assert(e.toString.contains("CustomTestFailureException"))
    }
  }

}
