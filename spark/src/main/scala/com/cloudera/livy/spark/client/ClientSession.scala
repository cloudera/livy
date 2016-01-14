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

package com.cloudera.livy.spark.client

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent
import java.util.concurrent.{ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.cloudera.livy.{Job, LivyClient}
import com.cloudera.livy.client.local.LocalClient
import com.cloudera.livy.sessions.{Session, SessionState}

class ClientSession(val sessionId: Int, createRequest: CreateClientRequest) extends Session {
  implicit val executionContext = ExecutionContext.global
  var sessionState: SessionState = SessionState.Starting()
  SessionClientTracker.createClient(
    sessionId, createRequest.sparkConf.asJava, createRequest.timeout)
  sessionState = SessionState.Running()

  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)

  def getClient(): Option[LocalClient] = {
    SessionClientTracker.getClient(id).map(_.asInstanceOf[LocalClient])
  }

  def runJob(job: Array[Byte]): Long = {
    performOperation(job, true)
  }

  def submitJob(job: Array[Byte]): Long = {
    performOperation(job, false)
  }

  def addFile(uri: URI): Unit = {
    getClient.foreach(_.addFile(uri))
  }

  def addJar(uri: URI): Unit = {
    getClient.foreach(_.addJar(uri))
  }

  def jobStatus(id: Long) = {
    throw new UnsupportedOperationException()
  }

  private def withClient[T](fn: (LocalClient => T)): T = {
    getClient() match {
      case Some(client) =>
        fn(client)
      case None =>
        throw new IllegalStateException(s"Client for session '$id' not found.")
    }
  }

  private def performOperation(job: Array[Byte], sync: Boolean): Long = withClient { client =>
    val future = client.bypass(ByteBuffer.wrap(job), sync)
    val opId = operationCounter.incrementAndGet()
    operations(opId) = future
    opId
   }

  override def id: Int = sessionId

  override def stop(): Future[Unit] = {
    Future {
      sessionState = SessionState.ShuttingDown()
      SessionClientTracker.closeSession(id)
      sessionState = SessionState.Dead()
    }
  }

  // TODO: Add support for this in RSC
  override def logLines(): IndexedSeq[String] = {
    null
  }

  override def state: SessionState = sessionState
}
