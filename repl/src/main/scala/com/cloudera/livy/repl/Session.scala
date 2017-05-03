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

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.Map.Entry
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
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

  private val interpreterExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private val cancelExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private implicit val formats = DefaultFormats

  @volatile private[repl] var _sc: Option[SparkContext] = None

  private var _state: SessionState = SessionState.NotStarted()

  // Number of statements kept in driver's memory
  private val numRetainedStatements = livyConf.getInt(RSCConf.Entry.RETAINED_STATEMENT_NUMBER)

  private val _statements = new JLinkedHashMap[Int, Statement] {
    protected override def removeEldestEntry(eldest: Entry[Int, Statement]): Boolean = {
      size() > numRetainedStatements
    }
  }.asScala

  private val newStatementId = new AtomicInteger(0)

  stateChangedCallback(_state)

  def start(): Future[SparkContext] = {
    val future = Future {
      changeState(SessionState.Starting())
      val sc = interpreter.start()
      _sc = Option(sc)
      changeState(SessionState.Idle())
      sc
    }(interpreterExecutor)

    future.onFailure { case _ => changeState(SessionState.Error()) }(interpreterExecutor)
    future
  }

  def kind: String = interpreter.kind

  def state: SessionState = _state

  def statements: collection.Map[Int, Statement] = _statements.synchronized {
    _statements.toMap
  }

  def execute(code: String): Int = {
    val statementId = newStatementId.getAndIncrement()
    val statement = new Statement(statementId, StatementState.Waiting, null)
    _statements.synchronized { _statements(statementId) = statement }

    Future {
      statement.compareAndTransit(StatementState.Waiting, StatementState.Running)

      if (statement.state.get() == StatementState.Running) {
        statement.output = executeCode(statementId, code)
      }

      statement.compareAndTransit(StatementState.Running, StatementState.Available)
      statement.compareAndTransit(StatementState.Cancelling, StatementState.Cancelled)
      statement.updateProgress(1.0)
    }(interpreterExecutor)

    statementId
  }

  def cancel(statementId: Int): Unit = {
    val statementOpt = _statements.synchronized { _statements.get(statementId) }
    if (statementOpt.isEmpty) {
      return
    }

    val statement = statementOpt.get
    if (statement.state.get().isOneOf(
      StatementState.Available, StatementState.Cancelled, StatementState.Cancelling)) {
      return
    } else {
      // statement 1 is running and statement 2 is waiting. User cancels
      // statement 2 then cancels statement 1. The 2nd cancel call will loop and block the 1st
      // cancel call since cancelExecutor is single threaded. To avoid this, set the statement
      // state to cancelled when cancelling a waiting statement.
      statement.compareAndTransit(StatementState.Waiting, StatementState.Cancelled)
      statement.compareAndTransit(StatementState.Running, StatementState.Cancelling)
    }

    info(s"Cancelling statement $statementId...")

    Future {
      val deadline = livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TIMEOUT).millis.fromNow

      while (statement.state.get() == StatementState.Cancelling) {
        if (deadline.isOverdue()) {
          info(s"Failed to cancel statement $statementId.")
          statement.compareAndTransit(StatementState.Cancelling, StatementState.Cancelled)
        } else {
          _sc.foreach(_.cancelJobGroup(statementId.toString))
          if (statement.state.get() == StatementState.Cancelling) {
            Thread.sleep(livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TRIGGER_INTERVAL))
          }
        }
      }

      if (statement.state.get() == StatementState.Cancelled) {
        info(s"Statement $statementId cancelled.")
      }
    }(cancelExecutor)
  }

  def close(): Unit = {
    interpreterExecutor.shutdown()
    cancelExecutor.shutdown()
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
      interpreter.execute(executionCount, code) match {
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
