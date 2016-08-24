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

import com.cloudera.livy.LivyConf
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.sessions.Session.RecoveryMetadata
import com.cloudera.livy.util.LineBufferedProcess
import com.cloudera.livy.utils.{AppInfo, SparkApp, SparkAppListener, SparkProcessBuilder}

@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchRecoveryMetadata(
    id: Int,
    appTag: String,
    appId: Option[String],
    owner: String,
    proxyUser: Option[String],
    version: Int = 1)
  extends RecoveryMetadata

object BatchSession {
  val RECOVERY_SESSION_TYPE = "batch"

  def create(
      id: Int,
      owner: String,
      proxyUser: Option[String],
      livyConf: LivyConf,
      createRequest: CreateBatchRequest,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    val appTag = s"livy-batch-$id-${Random.alphanumeric.take(8).mkString}"
    new BatchSession(
      id, appTag, owner, proxyUser, livyConf, Left(createRequest), sessionStore, mockApp)
  }

  def recover(
      m: BatchRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    new BatchSession(
      m.id, m.appTag, m.owner, m.proxyUser, livyConf, Right(m.appId), sessionStore, mockApp)
  }
}

class BatchSession(
    id: Int,
    appTag: String,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    // To create a new session, pass Left(createRequest). To recover a session, pass Right(appId).
    createOrRecover: Either[CreateBatchRequest, Option[String]],
    sessionStore: SessionStore,
    mockApp: Option[SparkApp]) // For unit test.
  extends Session(id, owner, livyConf) with SparkAppListener {
  import BatchSession._

  private val app = mockApp.getOrElse {
    val (appId, sparkSubmit): (Option[String], Option[LineBufferedProcess]) = {
      def create(request: CreateBatchRequest) = {
        val conf = SparkApp.prepareSparkConf(
          appTag,
          livyConf,
          prepareConf(request.conf, request.jars, request.files, request.archives, request.pyFiles))
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

        // Spark 1.x does not support specifying deploy mode in conf and needs special handling.
        livyConf.sparkDeployMode().foreach(builder.deployMode)

        sessionStore.save(BatchSession.RECOVERY_SESSION_TYPE, recoveryMetadata)

        builder.redirectOutput(Redirect.PIPE)
        builder.redirectErrorStream(true)

        val file = resolveURIs(Seq(request.file))(0)
        val sparkSubmit = builder.start(Some(file), request.args)

        (None, Option(sparkSubmit))
      }

      def recover(appId: Option[String]) = (appId, None)

      createOrRecover.fold(create, recover)
    }

    SparkApp.create(appTag, appId, sparkSubmit, livyConf, Some(this))
  }

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState =
    createOrRecover.fold(_ => SessionState.Starting(), _ => SessionState.Recovering())

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = app.log()

  override def stopSession(): Unit = {
    app.kill()
    sessionStore.remove(RECOVERY_SESSION_TYPE, id)
  }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)

    // Update state store if we are creating, or if we are recovering but didn't have the appId.
    if (createOrRecover.isLeft || createOrRecover.right.get.isEmpty) {
      sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
    }
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.RUNNING => _state = SessionState.Running()
        case SparkApp.State.FINISHED => _state = SessionState.Success()
        case SparkApp.State.KILLED | SparkApp.State.FAILED =>
          _state = SessionState.Dead()
        case _ =>
      }
    }
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }

  override def recoveryMetadata: RecoveryMetadata =
    BatchRecoveryMetadata(id, appTag, appId, owner, proxyUser)
}
