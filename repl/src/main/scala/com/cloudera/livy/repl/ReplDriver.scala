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
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

import io.netty.channel.ChannelHandlerContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import com.cloudera.livy.{JobContext, Logging}
import com.cloudera.livy.rsc.{BaseProtocol, RSCConf}
import com.cloudera.livy.rsc.driver.RSCDriver
import com.cloudera.livy.rsc.rpc.Rpc
import com.cloudera.livy.sessions._

class ReplDriver(conf: SparkConf, livyConf: RSCConf)
  extends RSCDriver(conf, livyConf)
  with Logging {

  private val jobFutures = mutable.Map[String, JValue]()

  private[repl] var session: Session = _

  override protected def initializeContext(): JavaSparkContext = {
    val interpreter = Kind(livyConf.get(RSCConf.Entry.SESSION_KIND)) match {
      case PySpark() => PythonInterpreter(conf)
      case Spark() => new SparkInterpreter(conf)
      case SparkSql() => new SparkSqlInterpreter(conf)
      case SparkR() => SparkRInterpreter(conf)
    }

    session = new Session(interpreter)
    Option(Await.result(session.start(), Duration.Inf))
      .map(new JavaSparkContext(_))
      .orNull
  }

  override protected def shutdownContext(): Unit = {
    if (session != null) {
      try {
        session.close()
      } finally {
        super.shutdownContext()
      }
    }
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.ReplJobRequest): Unit = {
    Future {
      jobFutures(msg.id) = session.execute(msg.code).result
    }
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplJobResult): String = {
    val result = jobFutures.getOrElse(msg.id, null)
    Option(result).map { r => compact(render(r)) }.orNull
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplState): String = {
    return session.state.toString
  }

}
