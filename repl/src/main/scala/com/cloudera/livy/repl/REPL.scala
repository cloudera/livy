/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.livy.repl

import com.cloudera.livy.client.local.BaseProtocol.REPLJobRequest
import com.cloudera.livy.client.local.driver.{Driver, MonitorCallback}
import com.cloudera.livy.repl.python.PythonInterpreter
import com.cloudera.livy.repl.scalaRepl.SparkInterpreter
import com.cloudera.livy.repl.sparkr.SparkRInterpreter
import org.json4s.JsonAST.{JNull, JValue}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class REPL(args: Array[String]) extends Driver(args) {
  val PYSPARK_SESSION = "pyspark"
  val SPARK_SESSION = "spark"
  val SPARKR_SESSION = "sparkr"
  val jobFutures = mutable.Map[String, JValue]()

  val interpreter = getLivyConf.get("session.kind") match {
    case PYSPARK_SESSION => PythonInterpreter()
    case SPARK_SESSION => SparkInterpreter()
    case SPARKR_SESSION => SparkRInterpreter()
  }

  val session = Session(interpreter)

  def shutdown(error: Throwable): Unit = {
    session.close()
  }

  def setMonitorCallback(bc: MonitorCallback): Unit = {
    // no op
  }

  def run(jobRequest: REPLJobRequest): Unit = {
    Future {
      jobFutures(jobRequest.id) = session.execute(jobRequest.code).result
    }
  }

  def getJobStatus(id: String): JValue = {
    jobFutures.getOrElse(id, JNull)
  }
}
