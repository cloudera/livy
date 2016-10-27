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

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.SparkContext
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import com.cloudera.livy.Logging
import com.cloudera.livy.rsc.driver.{Statement, StatementState}
import com.cloudera.livy.sessions._

object Session {
  val STATUS = "status"
  val OK = "ok"
  val ERROR = "error"
  val EXECUTION_COUNT = "execution_count"
  val DATA = "data"
  val ENAME = "ename"
  val EVALUE = "evalue"
  val TRACEBACK = "traceback"
}

class Session(interpreter: Interpreter)
  extends Logging
{
  import Session._

  private implicit val executor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())
  private implicit val formats = DefaultFormats

  private var _state: SessionState = SessionState.NotStarted()
  private val _statements = mutable.Map[Int, Statement]()

  private val newStatementId = new AtomicInteger(0)

  def start(): Future[SparkContext] = {
    val future = Future {
      _state = SessionState.Starting()
      val sc = interpreter.start()
      _state = SessionState.Idle()
      sc
    }
    future.onFailure { case _ =>
      _state = SessionState.Error(System.currentTimeMillis())
    }
    future
  }

  def kind: String = interpreter.kind

  def state: SessionState = _state

  def statements: mutable.Map[Int, Statement] = _statements

  def execute(code: String): Int = {
    val statementId = newStatementId.getAndIncrement()
    _statements.synchronized {
      _statements(statementId) = new Statement(statementId, StatementState.Waiting, null)
    }
    Future {
      _statements.synchronized {
        _statements(statementId) = new Statement(statementId, StatementState.Running, null)
      }

      val statement =
        new Statement(statementId, StatementState.Available, executeCode(statementId, code))

      _statements.synchronized {
        _statements(statementId) = statement
      }
    }
    statementId
  }

  def close(): Unit = {
    executor.shutdown()
    interpreter.close()
  }

  def clearStatements(): Unit = synchronized {
    _statements.clear()
  }

  private def executeCode(executionCount: Int, code: String): String = {
    _state = SessionState.Busy()

    val resultInJson = try {
      interpreter.execute(code) match {
        case Interpreter.ExecuteSuccess(data) =>
          _state = SessionState.Idle()

          (STATUS -> OK) ~
          (EXECUTION_COUNT -> executionCount) ~
          (DATA -> data)

        case Interpreter.ExecuteIncomplete() =>
          _state = SessionState.Idle()

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> "Error") ~
          (EVALUE -> "incomplete statement") ~
          (TRACEBACK -> List())

        case Interpreter.ExecuteError(ename, evalue, traceback) =>
          _state = SessionState.Idle()

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> ename) ~
          (EVALUE -> evalue) ~
          (TRACEBACK -> traceback)

        case Interpreter.ExecuteAborted(message) =>
          _state = SessionState.Error(System.nanoTime())

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> "Error") ~
          (EVALUE -> f"Interpreter died:\n$message") ~
          (TRACEBACK -> List())
      }
    } catch {
      case e: Throwable =>
        error("Exception when executing code", e)

        _state = SessionState.Idle()

        (STATUS -> ERROR) ~
        (EXECUTION_COUNT -> executionCount) ~
        (ENAME -> f"Internal Error: ${e.getClass.getName}") ~
        (EVALUE -> e.getMessage) ~
        (TRACEBACK -> List())
    }

    compact(render(resultInJson))
  }
}
