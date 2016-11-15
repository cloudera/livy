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

package com.cloudera.livy.repl

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}


import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar.mock
import org.scalatest.time._

import com.cloudera.livy.LivyBaseUnitTestSuite
import com.cloudera.livy.repl.Interpreter.ExecuteResponse

class SessionSpec extends FunSpec with Eventually with LivyBaseUnitTestSuite {
  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Millis)))

  describe("Session") {
    it("should call state changed callbacks in happy path") {
      val expectedStateTransitions = Array("not_started", "starting", "idle", "busy", "idle")
      val actualStateTransitions = new ConcurrentLinkedQueue[String]()

      val interpreter = mock[Interpreter]
      val session = new Session(interpreter, { s => actualStateTransitions.add(s.toString) })

      session.start()

      session.execute("")

      eventually {
        actualStateTransitions.toArray shouldBe expectedStateTransitions
      }
    }

    it("should not transit to idle if there're any pending statements.") {
      val expectedStateTransitions = Array("not_started", "busy", "busy", "idle")
      val actualStateTransitions = new ConcurrentLinkedQueue[String]()

      val interpreter = mock[Interpreter]
      val blockFirstExecuteCall = new CountDownLatch(1)
      when(interpreter.execute("")).thenAnswer(new Answer[Interpreter.ExecuteResponse] {
        override def answer(invocation: InvocationOnMock): ExecuteResponse = {
          blockFirstExecuteCall.await(10, TimeUnit.SECONDS)
          null
        }
      })
      val session = new Session(interpreter, { s => actualStateTransitions.add(s.toString) })

      for (_ <- 1 to 2) {
        session.execute("")
      }

      blockFirstExecuteCall.countDown()
      eventually {
        actualStateTransitions.toArray shouldBe expectedStateTransitions
      }
    }
  }
}
