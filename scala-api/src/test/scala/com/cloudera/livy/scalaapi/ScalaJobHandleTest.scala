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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.Success

import com.cloudera.livy.JobHandle


class ScalaJobHandleTest extends FunSuite with ScalaFutures with BeforeAndAfter {

  private var mockJobHandle: JobHandle[String] = null
  private var scalaJobHandle: ScalaJobHandle[String] = null
  private val timeoutInMilliseconds = 5000
  private var listener: JobHandle.Listener[String] = null

  before {
    listener = mock(classOf[JobHandle.Listener[String]])
    mockJobHandle = mock(classOf[JobHandle[String]])
    scalaJobHandle = new ScalaJobHandle(mockJobHandle)
  }

  test("get result when job is already complete") {
    when(mockJobHandle.get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)).thenReturn("hello")
    val result = Await.result(scalaJobHandle, 5 seconds)
    assert(result == "hello")
    verify(mockJobHandle, times(1)).get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)
  }

  test("ready when the thread waits for the mentioned duration for job to complete") {
    when(mockJobHandle.get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)).thenReturn("hello")
    val result = Await.ready(scalaJobHandle, 5 seconds)
    assert(result == scalaJobHandle)
    verify(mockJobHandle, times(1)).get(timeoutInMilliseconds, TimeUnit.MILLISECONDS)
  }

  test("ready with Infinite Duration") {
    when(mockJobHandle.isDone).thenReturn(true)
    when(mockJobHandle.get()).thenReturn("hello")
    val result = Await.ready(scalaJobHandle, Duration.Undefined)
    assert(result == scalaJobHandle)
    verify(mockJobHandle, times(1)).get()
  }

  test("onComplete") {
    doNothing().when(mockJobHandle).addListener(listener)
    scalaJobHandle onComplete {
      case Success(t) => {}
    }
    verify(mockJobHandle).addListener(isA(classOf[JobHandle.Listener[String]]))
    verify(mockJobHandle, times(1)).addListener(any())
  }

  test("onJobCancelled") {
    doNothing().when(mockJobHandle).addListener(listener)
    scalaJobHandle onJobCancelled {
      case true => {}
    }
    verify(mockJobHandle).addListener(isA(classOf[JobHandle.Listener[String]]))
    verify(mockJobHandle, times(1)).addListener(any())
  }
}
