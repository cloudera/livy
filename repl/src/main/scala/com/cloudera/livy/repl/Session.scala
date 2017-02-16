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

import scala.collection.mutable.LinkedHashMap
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
  private val _statements = LinkedHashMap[Int, Statement]()

  private val newStatementId = new AtomicInteger(0)

  // Number of statements kept in driver's memory
  private val numRetainedStatements = livyConf.getInt(RSCConf.Entry.RETAINED_STATEMENT_NUMBER)

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
      setJobGroup(statementId)
      statement.compareAndTransition(StatementState.Waiting, StatementState.Running)

      statement.checkStateAndExecute(StatementState.Running,
        new Runnable {
          override def run(): Unit = {
            _statements(statementId).output = executeCode(statementId, code)
          }
        })

      statement.compareAndTransition(StatementState.Running, StatementState.Available)
      statement.compareAndTransition(StatementState.Cancelling, StatementState.Cancelled)

      // Clean old statements
      cleanOldStatements()
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
      statement.compareAndTransition(StatementState.Waiting, StatementState.Cancelled)
      statement.compareAndTransition(StatementState.Running, StatementState.Cancelling)
    }

    info(s"Cancelling statement $statementId...")

    Future {
      val deadline = livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TIMEOUT).millis.fromNow
      val runnable = new Runnable {
        override def run(): Unit = {
          if (deadline.isOverdue()) {
            info(s"Failed to cancel statement $statementId.")
            statement.compareAndTransition(StatementState.Cancelling, StatementState.Cancelled)
          } else {
            _sc.foreach(_.cancelJobGroup(statementId.toString))
          }
          Thread.sleep(livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TRIGGER_INTERVAL))
        }
      }

      while (statement.checkStateAndExecute(StatementState.Cancelling, runnable)) { }
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

  private def cleanOldStatements(): Unit = _statements.synchronized {
    if (_statements.size > numRetainedStatements) {
      // Remove 10% of existing statements to avoid frequently calling this method when reaching
      // threshold.
      val toRemove = (numRetainedStatements * 0.1).toInt + 1
      (0 until toRemove).foreach { _ =>
        _statements.head._2.state.get() match {
          case StatementState.Available | StatementState.Cancelled =>
          // Only remove statement that is finished.
          _statements -= _statements.head._1
          case _ => // Unit
        }
      }
    }
  }
}
