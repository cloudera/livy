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

import java.util.UUID

import org.apache.spark.SparkContext
import org.json4s.JObject

import com.cloudera.livy.Logging

object Interpreter {
  abstract class ExecuteResponse

  case class ExecuteSuccess(content: JObject) extends ExecuteResponse
  case class ExecuteError(ename: String,
                          evalue: String,
                          traceback: Seq[String] = Seq()) extends ExecuteResponse
  case class ExecuteIncomplete() extends ExecuteResponse
  case class ExecuteAborted(message: String) extends ExecuteResponse
}

trait Interpreter extends Logging {
  import Interpreter._

  protected var sparkContext: SparkContext = _

  def kind: String

  /**
   * Start the Interpreter.
   *
   * @return A SparkContext, which may be null.
   */
  def start(): SparkContext = {
    sparkContext = internalStart()
    sparkContext
  }

  def internalStart(): SparkContext

  /**
    * Execute the code and return the result as a Future as it may
    * take some time to execute. Use UUID as the jobGroupId. This is just for testing. In reality,
    * The statementId will be used as the JobGroupId.
    */
  def execute(code: String): ExecuteResponse = execute(code, UUID.randomUUID().toString)

  /**
   * Execute the code and return the result as a Future as it may
   * take some time to execute. Use the statementId as the jobGroupId.
   */
  def execute(code: String, statementId: String): ExecuteResponse = {
    sparkContext.setJobGroup(statementId, s"job group id for statement ${statementId}")
    internalExecute(code)
  }


  def internalExecute(code: String): ExecuteResponse

  def cancel(statementId: String): Unit = {
    info(s"Statement: ${statementId} is canceled.")
    sparkContext.cancelJobGroup(statementId)
  }

  /** Shut down the interpreter. */
  def close(): Unit
}
