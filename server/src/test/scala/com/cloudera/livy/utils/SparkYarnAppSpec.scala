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
package com.cloudera.livy.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.Exception.noCatch

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus.UNDEFINED
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.LivyConf
import com.cloudera.livy.util.LineBufferedProcess
import com.cloudera.livy.utils.SparkApp._

class SparkYarnAppSpec extends FunSpec {
  private def cleanupThread(t: Thread)(f: => Unit) = {
    noCatch.andFinally { t.interrupt() } (f)
  }

  private def mockSleep(ms: Long) = {
    Thread.`yield`()
  }

  private def withSleepMethod(mockSleep: Long => Unit)(f: => Unit): Unit = {
    noCatch.andFinally { Clock.setSleepMethod(Thread.sleep) } {
      Clock.setSleepMethod(mockSleep)
      f
    }
  }

  describe("SparkYarnApp") {
    val TEST_TIMEOUT = 30 seconds
    val appId = ConverterUtils.toApplicationId("application_1467912463905_0021")
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "0")

    it("should poll YARN state and terminate") {
      withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockAppListener = mock[SparkAppListener]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        // Simulate YARN app state progression.
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          private var stateSeq = List(ACCEPTED, RUNNING, FINISHED)

          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            val currentState = stateSeq.head
            if (stateSeq.tail.nonEmpty) {
              stateSeq = stateSeq.tail
            }
            currentState
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val app = new SparkYarnApp(appId, None, Some(mockAppListener), livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockAppListener).stateChanged(State.STARTING, State.RUNNING)
          verify(mockAppListener).stateChanged(State.RUNNING, State.FINISHED)
        }
      }
    }

    it("should kill yarn app") {
      withSleepMethod(mockSleep) {
        val diag = "DIAG"
        val mockYarnClient = mock[YarnClient]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getDiagnostics).thenReturn(diag)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)

        var appKilled = false
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            if (!appKilled) {
              RUNNING
            } else {
              KILLED
            }
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val app = new SparkYarnApp(appId, None, None, livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          app.kill()
          appKilled = true

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockYarnClient).killApplication(appId)
          assert(app.log().mkString.contains(diag))
        }
      }
    }

    it("should return spark-submit log") {
      withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockSparkSubmit = mock[LineBufferedProcess]
        val sparkSubmitLog = IndexedSeq("SPARK-SUBMIT", "LOG")
        when(mockSparkSubmit.inputLines).thenReturn(sparkSubmitLog)
        val waitForCalledLatch = new CountDownLatch(1)
        when(mockSparkSubmit.waitFor()).thenAnswer(new Answer[Int]() {
          override def answer(invocation: InvocationOnMock): Int = {
            waitForCalledLatch.countDown()
            1
          }
        })

        val app = new SparkYarnApp(appId, Some(mockSparkSubmit), None, livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          waitForCalledLatch.await(TEST_TIMEOUT.toMillis, TimeUnit.MILLISECONDS)
          assert(app.log() == sparkSubmitLog, "Expect spark-submit log")
        }
      }
    }

    it("can kill spark-submit while it's running") {
      withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockSparkSubmit = mock[LineBufferedProcess]
        when(mockSparkSubmit.exitValue()).thenReturn(1)

        val sparkSubmitRunningLatch = new CountDownLatch(1)
        // Simulate a running spark-submit
        when(mockSparkSubmit.inputLines).thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            sparkSubmitRunningLatch.await()
          }
        })

        def appId: ApplicationId = { throw new Exception("fake") }
        val app = new SparkYarnApp(appId, Some(mockSparkSubmit), None, livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          app.kill()
          verify(mockSparkSubmit, times(1)).destroy()
        }
      }
    }

    it("should map YARN state to SparkApp.State correctly") {
      val app = new SparkYarnApp(appId, None, None, livyConf)
      cleanupThread(app.yarnAppMonitorThread) {
        assert(app.mapYarnState(appId, NEW, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, NEW_SAVING, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, SUBMITTED, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, ACCEPTED, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, RUNNING, UNDEFINED) == State.RUNNING)
        assert(
          app.mapYarnState(appId, FINISHED, FinalApplicationStatus.SUCCEEDED) == State.FINISHED)
        assert(app.mapYarnState(appId, FINISHED, FinalApplicationStatus.FAILED) == State.FAILED)
        assert(app.mapYarnState(appId, FINISHED, FinalApplicationStatus.KILLED) == State.KILLED)
        assert(app.mapYarnState(appId, FINISHED, UNDEFINED) == State.FAILED)
        assert(app.mapYarnState(appId, FAILED, UNDEFINED) == State.FAILED)
        assert(app.mapYarnState(appId, KILLED, UNDEFINED) == State.KILLED)
      }
    }
  }
}
