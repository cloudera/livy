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
import scala.concurrent.duration._

import org.apache.spark.SparkContext
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import com.cloudera.livy.Logging
import com.cloudera.livy.rsc.RSCConf
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

class Session(
    livyConf: RSCConf,
    interpreter: Interpreter,
    stateChangedCallback: SessionState => Unit = { _ => })
  extends Logging {
  import Session._

  private implicit val executor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private val cancelExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private implicit val formats = DefaultFormats

  @volatile private[repl] var _sc: Option[SparkContext] = None

  private var _state: SessionState = SessionState.NotStarted()
  private val _statements = TrieMap[Int, Statement]()

  private val newStatementId = new AtomicInteger(0)

  stateChangedCallback(_state)

  def start(): Future[SparkContext] = {
    val future = Future {
      changeState(SessionState.Starting())
      val sc = interpreter.start()
      _sc = Option(sc)
      changeState(SessionState.Idle())
      sc
    }

    future.onFailure { case _ => changeState(SessionState.Error()) }
    future
  }

  def kind: String = interpreter.kind

  def state: SessionState = _state

  def statements: collection.Map[Int, Statement] = _statements.readOnlySnapshot()

  def execute(code: String): Int = {
    val statementId = newStatementId.getAndIncrement()
    _statements(statementId) = new Statement(statementId, StatementState.Waiting, null)

    Future {
      setJobGroup(statementId)
      _statements(statementId).state.compareAndSet(StatementState.Waiting, StatementState.Running)

      val executeResult = if (_statements(statementId).state.get() != StatementState.Cancelling) {
        executeCode(statementId, code)
      } else {
        null
      }

      _statements(statementId).output = executeResult
      _statements(statementId).state.compareAndSet(StatementState.Running, StatementState.Available)
      _statements(statementId).state.compareAndSet(
        StatementState.Cancelling, StatementState.Cancelled)
    }

    statementId
  }

  def cancel(statementId: Int): Unit = {
    if (!_statements.contains(statementId)) {
      return
    }

    if (_statements(statementId).state.get() == StatementState.Available ||
      _statements(statementId).state.get() == StatementState.Cancelled ||
      _statements(statementId).state.get() == StatementState.Cancelling) {
      return
    } else {
      _statements(statementId).state.getAndSet(StatementState.Cancelling)
    }

    info(s"Cancelling statement $statementId...")

      Future {
        val deadline = livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TIMEOUT).millis.fromNow
        while (_statements(statementId).state.get() == StatementState.Cancelling) {
          if (deadline.isOverdue()) {
            info(s"Failed to cancel statement $statementId.")
            _statements(statementId).state.compareAndSet(
              StatementState.Cancelling, StatementState.Cancelled)
          } else {
            _sc.foreach(_.cancelJobGroup(statementId.toString))
          }
          Thread.sleep(livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TRIGGER_INTERVAL))
        }
        if (_statements(statementId).state.get() == StatementState.Cancelled) {
          info(s"Statement $statementId cancelled.")
        }
      }(cancelExecutor)
  }

  def close(): Unit = {
    executor.shutdown()
    interpreter.close()
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

  private def setJobGroup(statementId: Int): String = {
    val cmd = Kind(interpreter.kind) match {
      case Spark() =>
        // A dummy value to avoid automatic value binding in scala REPL.
        s"""val _livyJobGroup$statementId = sc.setJobGroup("$statementId",""" +
          s""""Job group for statement $statementId")"""
      case PySpark() | PySpark3() =>
        s"""sc.setJobGroup("$statementId", "Job group for statement $statementId")"""
      case SparkR() =>
        interpreter.asInstanceOf[SparkRInterpreter].sparkMajorVersion match {
          case "1" =>
            s"""setJobGroup(sc, "$statementId", "Job group for statement $statementId", """ +
              "FALSE)"
          case "2" =>
            s"""setJobGroup("$statementId", "Job group for statement $statementId", FALSE)"""
        }
    }
    // Set the job group
    executeCode(statementId, cmd)
  }
}
