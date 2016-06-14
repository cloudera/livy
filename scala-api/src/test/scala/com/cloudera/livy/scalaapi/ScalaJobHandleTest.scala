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
import scala.util.{Failure, Success}

import com.cloudera.livy.JobHandle
import com.cloudera.livy.JobHandle.{Listener, State}

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

  test("verify addListener call of java jobHandle for onComplete") {
    doNothing().when(mockJobHandle).addListener(listener)
    scalaJobHandle onComplete {
      case Success(t) => {}
    }
    verify(mockJobHandle).addListener(isA(classOf[Listener[String]]))
    verify(mockJobHandle, times(1)).addListener(any())
  }

  test("onComplete Success") {
    val jobHandleStub = new AbstractJobHandleStub[String] {
      override def addListener(l: Listener[String]): Unit = l.onJobSucceeded(this, "hello")
    }
    val lock = new Object
    var testFailure: Option[String] = None
    val testScalaHandle = new ScalaJobHandle(jobHandleStub)
    lock.synchronized {
      testScalaHandle onComplete {
        case Success(t) => {
          if (!t.equals("hello")) {
            testFailure = Some("onComplete has not returned the expected message")
          }
          ScalaClientTest.notify(lock)
        }
        case Failure(e) => {
          testFailure = Some("onComplete should not have triggered Failure callback")
          ScalaClientTest.notify(lock)
        }
      }
    }
    ScalaClientTest.wait(lock)
    testFailure.foreach(fail(_))
  }

  test("onComplete Failure") {
    val jobHandleStub = new AbstractJobHandleStub[String] {
      override def addListener(l: Listener[String]): Unit =
        l.onJobFailed(this, new CustomTestFailureException)

      override def get(): String = throw new CustomTestFailureException()
    }
    val lock = new Object
    var testFailure: Option[String] = None
    val testScalaHandle = new ScalaJobHandle(jobHandleStub)
    lock.synchronized {
      testScalaHandle onComplete {
        case Success(t) => {
          testFailure = Some("Test should have thrown CustomFailureException")
          ScalaClientTest.notify(lock)
        }
        case Failure(e) => {
          if (!e.isInstanceOf[CustomTestFailureException]) {
            testFailure = Some("Test did not throw expected exception - CustomFailureException")
          }
          ScalaClientTest.notify(lock)
        }
      }
    }
    ScalaClientTest.wait(lock)
    testFailure.foreach(fail(_))
  }

  test("onJobCancelled") {
    val jobHandleStub = new AbstractJobHandleStub[String] {
      override def addListener(l: Listener[String]): Unit = l.onJobCancelled(this)
      override def cancel(mayInterruptIfRunning: Boolean): Boolean = true
    }
    var testFailure: Option[String] = None
    val lock = new Object
    val testScalaHandle = new ScalaJobHandle(jobHandleStub)
    lock.synchronized {
      testScalaHandle onJobCancelled  {
        case true => ScalaClientTest.notify(lock)
        case false => {
          testFailure = Some("False callback should not have been triggered")
          ScalaClientTest.notify(lock)
        }
      }
    }
    ScalaClientTest.wait(lock)
    testFailure.foreach(fail(_))
  }

  test("onJobQueued") {
    val jobHandleStub = new AbstractJobHandleStub[String] {
      override def addListener(l: Listener[String]): Unit = l.onJobQueued(this)
    }
    var hasTestPassed = false
    val lock = new Object
    val testScalaHandle = new ScalaJobHandle(jobHandleStub)
    lock.synchronized {
      testScalaHandle onJobQueued {
        hasTestPassed = true
        ScalaClientTest.notify(lock)
      }
    }
    ScalaClientTest.wait(lock)
    if (!hasTestPassed) fail("Callback not triggered")
  }

  test("onJobStarted") {
    val jobHandleStub = new AbstractJobHandleStub[String] {
      override def addListener(l: Listener[String]): Unit = l.onJobStarted(this)
    }
    var hasTestPassed = false
    val lock = new Object
    val testScalaHandle = new ScalaJobHandle(jobHandleStub)
    lock.synchronized {
      testScalaHandle onJobStarted {
        hasTestPassed = true
        ScalaClientTest.notify(lock)
      }
    }
    ScalaClientTest.wait(lock)
    if (!hasTestPassed) fail("Callback not triggered")
  }
}

private abstract class AbstractJobHandleStub[T] private[livy] extends JobHandle[T] {

  override def getState: State = null

  override def addListener(l: Listener[T]): Unit = {}

  override def isCancelled: Boolean = false

  override def get(): T = null.asInstanceOf[T]

  override def get(timeout: Long, unit: TimeUnit): T = null.asInstanceOf[T]

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = false

  override def isDone: Boolean = true
}
