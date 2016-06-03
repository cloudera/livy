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

import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import com.cloudera.livy.JobHandle

class ScalaJobHandleTest extends FunSuite with ScalaFutures {

  val jobHandle = mock(classOf[JobHandle[String]])
  val listener = mock(classOf[JobHandle.Listener[String]])
  val scalaJobHandle = new ScalaJobHandle(jobHandle)
  val timeoutInMilliseconds = 5000

  test("get result when job is already complete") {
    when(jobHandle.get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)).thenReturn("hello")
    val result = Await.result(scalaJobHandle, 5 seconds)
    assert(result == "hello")
    verify(jobHandle, times(1)).get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)
  }

  test("ready when the thread waits for the mentioned duration for job to complete") {
    when(jobHandle.isDone).thenReturn(false).thenReturn(true)
    val result = Await.ready(scalaJobHandle, 5 seconds)
    assert(result == scalaJobHandle)
    verify(jobHandle, times(2)).isDone
  }

  test("ready with Infinite Duration") {
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    val result = Await.ready(scalaJobHandle, Duration.Undefined)
    assert(result == scalaJobHandle)
    verify(jobHandle, times(1)).get()
  }

  test("onCompleteSucess with global ExecutionContext") {
    doNothing().when(jobHandle).addListener(listener)
    when(jobHandle.isDone).thenReturn(true)
    when(jobHandle.get()).thenReturn("hello")
    doNothing().when(listener).onJobSucceeded(jobHandle, "hello")
    scalaJobHandle.onComplete(onCompleteSuccessCallbackFunc)
    verify(jobHandle, times(1)).isDone
    verify(listener, times(1)).onJobSucceeded(jobHandle, "hello")
  }

  private def onCompleteSuccessCallbackFunc(callback: Try[String]) = callback match {
    case Success(t) => assert(t === "hello")
    case Failure(e) => fail("Should not trigger Failure callback in onCompleteSuccessCallbackFunc")
  }
}
