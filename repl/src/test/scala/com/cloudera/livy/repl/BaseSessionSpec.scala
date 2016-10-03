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

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.json4s.DefaultFormats
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.LivyBaseUnitTestSuite
import com.cloudera.livy.sessions.SessionState

abstract class BaseSessionSpec extends FlatSpec with Matchers with LivyBaseUnitTestSuite {

  implicit val formats = DefaultFormats

  def withSession(testCode: Session => Any): Unit = {
    val session = new Session(createInterpreter())
    try {
      Await.ready(session.start(), 30 seconds)
      assert(session.state === SessionState.Idle())
      testCode(session)
    } finally {
      session.close()
    }
  }

  def createInterpreter(): Interpreter

  it should "start in the starting or idle state" in {
    val session = new Session(createInterpreter())
    val future = session.start()
    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        session.state should (equal (SessionState.Starting()) or equal (SessionState.Idle()))
      }
      Await.ready(future, 60 seconds)
    } finally {
      session.close()
    }
  }

  it should "eventually become the idle state" in withSession { session =>
    session.state should equal (SessionState.Idle())
  }

}
