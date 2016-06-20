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

package com.cloudera.livy.server.batch

import java.lang.ProcessBuilder.Redirect

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.utils.SparkProcessBuilder

class BatchSession(
    id: Int,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateBatchRequest)
    extends Session(id, owner, livyConf) {

  private val process = {
    val conf = prepareConf(request.conf, request.jars, request.files, request.archives,
      request.pyFiles)
    require(request.file != null, "File is required.")

    val builder = new SparkProcessBuilder(livyConf)
    builder.conf(conf)
    proxyUser.foreach(builder.proxyUser)
    request.className.foreach(builder.className)
    request.driverMemory.foreach(builder.driverMemory)
    request.driverCores.foreach(builder.driverCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.executorCores.foreach(builder.executorCores)
    request.numExecutors.foreach(builder.numExecutors)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)
    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)

    val file = resolveURIs(Seq(request.file))(0)
    builder.start(Some(file), request.args)
  }

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = SessionState.Running()

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = process.inputLines

  override def stopSession(): Unit = destroyProcess()

  private def destroyProcess() = {
    if (process.isAlive) {
      process.destroy()
      reapProcess(process.waitFor())
    }
  }

  private def reapProcess(exitCode: Int) = synchronized {
    if (_state.isActive) {
      if (exitCode == 0) {
        _state = SessionState.Success()
      } else {
        _state = SessionState.Error()
      }
    }
  }

  /** Simple daemon thread to make sure we change state when the process exits. */
  private[this] val thread = new Thread("Batch Process Reaper") {
    override def run(): Unit = {
      reapProcess(process.waitFor())
    }
  }
  thread.setDaemon(true)
  thread.start()
}
