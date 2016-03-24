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

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.netty.channel.ChannelHandlerContext
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import com.cloudera.livy.client.local.BaseProtocol
import com.cloudera.livy.client.local.driver.{Driver, DriverProtocol, MonitorCallback}
import com.cloudera.livy.client.local.rpc.Rpc
import com.cloudera.livy.repl.python.PythonInterpreter
import com.cloudera.livy.repl.scalaRepl.SparkInterpreter
import com.cloudera.livy.repl.sparkr.SparkRInterpreter
import com.cloudera.livy.sessions._

class ReplProtocol(driver: ReplDriver, clientRpc: Rpc, jcLock: Object)
  extends DriverProtocol(driver, clientRpc, jcLock) {

  private def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.ReplJobRequest): Unit = {
    driver.run(msg.id, msg.code)
  }

  private def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplJobResult): String = {
    val result = driver.getJobStatus(msg.id)
    Option(result).map { r => compact(render(r)) }.orNull
  }

  private def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplState): String = {
    return driver.session.state.toString
  }

}

class ReplDriver(args: Array[String]) extends Driver(args) {
  private val jobFutures = mutable.Map[String, JValue]()

  private val interpreter = Kind(getLivyConf.get("session.kind")) match {
    case PySpark() => PythonInterpreter("pyspark")
    case PySpark3() => PythonInterpreter("pyspark3")
    case Spark() => SparkInterpreter()
    case SparkR() => SparkRInterpreter()
  }

  private[repl] val session = Session(interpreter)

  override def createProtocol(client: Rpc): DriverProtocol = {
    new ReplProtocol(this, client, jcLock)
  }

  override def shutdownDriver(): Unit = {
    session.close()
  }

  override def setMonitorCallback(bc: MonitorCallback): Unit = {
    // no op
  }

  def run(id: String, code: String): Unit = {
    Future {
      jobFutures(id) = session.execute(code).result
    }
  }

  def getJobStatus(id: String): JValue = {
    jobFutures.getOrElse(id, null)
  }

}

object ReplDriver {

  def main(args: Array[String]): Unit = new ReplDriver(args).run()

}
