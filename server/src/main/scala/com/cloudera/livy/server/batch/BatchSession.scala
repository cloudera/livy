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

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.sessions.Session._
import com.cloudera.livy.utils._

@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchRecoveryMetadata(
    id: Int,
    appId: Option[String],
    appTag: String,
    owner: String,
    proxyUser: Option[String],
    version: Int = 1)
  extends RecoveryMetadata

object BatchSession extends Logging {
  val RECOVERY_SESSION_TYPE = "batch"

  def create(
      id: Int,
      request: CreateBatchRequest,
      livyConf: LivyConf,
      owner: String,
      proxyUser: Option[String],
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    val appTag = s"livy-batch-$id-${Random.alphanumeric.take(8).mkString}"
    val sparkEnv = SparkEnvironment.getSparkEnv(livyConf, request.sparkEnv)

    def createSparkApp(s: BatchSession): SparkApp = {
      val conf = SparkApp.prepareSparkConf(
        appTag,
        livyConf,
        prepareConf(
          request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))
      require(request.file != null, "File is required.")

      val builder = new SparkProcessBuilder(livyConf)
      builder.conf(conf).executable(sparkEnv.sparkSubmit())

      proxyUser.foreach(builder.proxyUser)
      request.className.foreach(builder.className)
      request.driverMemory.foreach(builder.driverMemory)
      request.driverCores.foreach(builder.driverCores)
      request.executorMemory.foreach(builder.executorMemory)
      request.executorCores.foreach(builder.executorCores)
      request.numExecutors.foreach(builder.numExecutors)
      request.queue.foreach(builder.queue)
      request.name.foreach(builder.name)

      // Spark 1.x does not support specifying deploy mode in conf and needs special handling.
      livyConf.sparkDeployMode().foreach(builder.deployMode)

      sessionStore.save(BatchSession.RECOVERY_SESSION_TYPE, s.recoveryMetadata)

      builder.redirectOutput(Redirect.PIPE)
      builder.redirectErrorStream(true)

      val file = resolveURIs(Seq(request.file), livyConf)(0)
      val sparkSubmit = builder.start(Some(file), request.args)

      SparkApp.create(appTag, None, Option(sparkSubmit), livyConf, Option(s))
    }

    info(s"Creating batch session $id: [owner: $owner, request: $request]")

    new BatchSession(
      id,
      appTag,
      SessionState.Starting(),
      livyConf,
      owner,
      proxyUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse(createSparkApp))
  }

  def recover(
      m: BatchRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    new BatchSession(
      m.id,
      m.appTag,
      SessionState.Recovering(),
      livyConf,
      m.owner,
      m.proxyUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse { s =>
        SparkApp.create(m.appTag, m.appId, None, livyConf, Option(s))
      })
  }
}

class BatchSession(
    id: Int,
    appTag: String,
    initialState: SessionState,
    livyConf: LivyConf,
    owner: String,
    override val proxyUser: Option[String],
    sessionStore: SessionStore,
    sparkApp: BatchSession => SparkApp)
  extends Session(id, owner, livyConf) with SparkAppListener {
  import BatchSession._

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = initialState
  private val app = sparkApp(this)

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = app.log()

  override def stopSession(): Unit = {
    app.kill()
  }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
    sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.RUNNING =>
          _state = SessionState.Running()
          info(s"Batch session $id created [appid: ${appId.orNull}, state: ${state.toString}, " +
            s"info: ${appInfo.asJavaMap}]")
        case SparkApp.State.FINISHED => _state = SessionState.Success()
        case SparkApp.State.KILLED | SparkApp.State.FAILED =>
          _state = SessionState.Dead()
        case _ =>
      }
    }
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }

  override def recoveryMetadata: RecoveryMetadata =
    BatchRecoveryMetadata(id, appId, appTag, owner, proxyUser)
}
