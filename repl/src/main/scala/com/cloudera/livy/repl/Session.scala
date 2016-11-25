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

import scala.collection.concurrent.TrieMap
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

class Session(interpreter: Interpreter, stateChangedCallback: SessionState => Unit = { _ => })
  extends Logging {
  import Session._

  private implicit val executor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private implicit val formats = DefaultFormats

  @volatile private[repl] var _sc: Option[SparkContext] = None

  private var _state: SessionState = SessionState.NotStarted()
  private val _statements = TrieMap[Int, Statement]()

  private val newStatementId = new AtomicInteger(0)
  private val lock = new Object()

  stateChangedCallback(_state)

  def start(): Future[SparkContext] = {
    val future = Future {
      changeState(SessionState.Starting())
      val sc = interpreter.start()
      changeState(SessionState.Idle())
      sc
    }

    future.onFailure { case _ =>
      changeState(SessionState.Error())
    }
    future.onSuccess { case sc => _sc = Option(sc).orElse(Some(SparkContext.getOrCreate())) }

    future
  }

  def kind: String = interpreter.kind

  def state: SessionState = _state

  def statements: collection.Map[Int, Statement] = _statements.readOnlySnapshot()

  def execute(code: String): Int = {
    val statementId = newStatementId.getAndIncrement()
    _statements(statementId) = new Statement(statementId, StatementState.Waiting, null)

    Future {
      def stmtState = _statements(statementId).state

      if (stmtState != StatementState.Cancelled) {
        lock.synchronized {
          if (stmtState != StatementState.Cancelled) {
            _statements(statementId) = new Statement(statementId, StatementState.Running, null)
            _sc.foreach(
              _.setJobGroup(statementId.toString, s"Job group for statement $statementId"))
          }
        }
      }

      val executeResult = if (stmtState != StatementState.Cancelled) {
        executeCode(statementId, code)
      } else {
        null
      }

      if (stmtState != StatementState.Cancelled) {
        lock.synchronized {
          if (stmtState != StatementState.Cancelled) {
            _statements(statementId) =
              new Statement(statementId, StatementState.Available, executeResult)
          }
        }
      }
    }

    statementId
  }

  def cancel(statementId: Int): Unit = {
    if (_statements.contains(statementId)) lock.synchronized {
      val oldStmtState = _statements.put(statementId,
        new Statement(statementId, StatementState.Cancelled, null))
      info(s"Statement $statementId is canceled")

      if (oldStmtState.get.state == StatementState.Running) {
        _sc.foreach(_.cancelJobGroup(statementId.toString))
      }
    }
  }

  def close(): Unit = {
    executor.shutdown()
    interpreter.close()
  }

  def clearStatements(): Unit = {
    _statements.clear()
  }

  private def changeState(newState: SessionState): Unit = {
    synchronized {
      _state = newState
    }
    stateChangedCallback(newState)
  }

  private def executeCode(executionCount: Int, code: String): String = {
    changeState(SessionState.Busy())

    def transitToIdle() = {
      val executingLastStatement = executionCount == newStatementId.intValue() - 1
      if (_statements.isEmpty || executingLastStatement) {
        changeState(SessionState.Idle())
      }
    }

    val resultInJson = try {
      interpreter.execute(code) match {
        case Interpreter.ExecuteSuccess(data) =>
          transitToIdle()

          (STATUS -> OK) ~
          (EXECUTION_COUNT -> executionCount) ~
          (DATA -> data)

        case Interpreter.ExecuteIncomplete() =>
          transitToIdle()

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> "Error") ~
          (EVALUE -> "incomplete statement") ~
          (TRACEBACK -> Seq.empty[String])

        case Interpreter.ExecuteError(ename, evalue, traceback) =>
          transitToIdle()

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> ename) ~
          (EVALUE -> evalue) ~
          (TRACEBACK -> traceback)

        case Interpreter.ExecuteAborted(message) =>
          changeState(SessionState.Error())

          (STATUS -> ERROR) ~
          (EXECUTION_COUNT -> executionCount) ~
          (ENAME -> "Error") ~
          (EVALUE -> f"Interpreter died:\n$message") ~
          (TRACEBACK -> Seq.empty[String])
      }
    } catch {
      case e: Throwable =>
        error("Exception when executing code", e)

        transitToIdle()

        (STATUS -> ERROR) ~
        (EXECUTION_COUNT -> executionCount) ~
        (ENAME -> f"Internal Error: ${e.getClass.getName}") ~
        (EVALUE -> e.getMessage) ~
        (TRACEBACK -> Seq.empty[String])
    }

    compact(render(resultInJson))
  }
}
