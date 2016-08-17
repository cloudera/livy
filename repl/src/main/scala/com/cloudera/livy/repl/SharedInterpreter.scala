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

import org.apache.spark.{SparkConf, SparkContext}

import com.cloudera.livy.repl.Interpreter.ExecuteResponse
import com.cloudera.livy.sessions.PySpark

class SharedInterpreter(conf: SparkConf) extends Interpreter {

  private val interpreters: Map[String, Interpreter] = Map(
    "spark" -> new SparkInterpreter(conf),
    "pyspark" -> PythonInterpreter(conf, PySpark()),
    "sparkr" -> SparkRInterpreter(conf))

  override def kind: String = "shared"

  /**
   * Execute the code and return the result as a Future as it may
   * take some time to execute.
   * The input to SharedInterpreter should like '%kind code'
   * e.g. %spark 1+1
   */
  override def execute(code: String): ExecuteResponse = {
    require(code.startsWith("%"))
    val firstSpace = code.indexOf(' ')
    val codeType = code.substring(1, firstSpace)
    interpreters(codeType).execute(code.substring(firstSpace + 1))
  }

  /** Shut down the interpreter. */
  override def close(): Unit = {
    interpreters.foreach(_._2.close())
  }

  /**
   * Start the Interpreter.
   *
   */
  override def start(): SparkContext = {
    interpreters.foreach(_._2.start())
    interpreters("spark").asInstanceOf[SparkInterpreter].getSparkContext()
  }
}
