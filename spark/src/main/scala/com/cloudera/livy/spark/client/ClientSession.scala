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
package com.cloudera.livy.spark.client

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.cloudera.livy.{Logging, LivyClientBuilder}
import com.cloudera.livy.client.local.LocalClient
import com.cloudera.livy.sessions.{Session, SessionState}

class ClientSession(val sessionId: Int, createRequest: CreateClientRequest)
  extends Session with Logging {
  implicit val executionContext = ExecutionContext.global

  var sessionState: SessionState = SessionState.Starting()

  private val _timeout = createRequest.timeout

  private val client = {
    info("Creating LivyClient for sessionId: " + sessionId)
    val builder = new LivyClientBuilder(new URI("local:spark"))
    new SparkConf(true).getAll.foreach { case (k, v) => builder.setConf(k, v) }
    builder
      .setAll(createRequest.sparkConf.asJava)
      .setConf("livy.client.sessionId", sessionId.toString)
      .setIfMissing("spark.master", "yarn-cluster")
      .build()
  }
  info("Livy client created.")

  sessionState = SessionState.Running()

  private val operations = mutable.Map[Long, java.util.concurrent.Future[_]]()
  private val operationCounter = new AtomicLong(0)

  @volatile private var _lastActivity = System.currentTimeMillis()

  def runJob(job: Array[Byte]): Long = {
    performOperation(client => client.bypassSync(ByteBuffer.wrap(job)))
  }

  def submitJob(job: Array[Byte]): Long = {
    performOperation(client => client.bypass(ByteBuffer.wrap(job)))
  }

  def addFile(uri: URI): Unit = {
    _lastActivity = System.currentTimeMillis()
    client.addFile(uri)
  }

  def addJar(uri: URI): Unit = {
    _lastActivity = System.currentTimeMillis()
    client.addJar(uri)
  }

  def jobStatus(id: Long) = {
    _lastActivity = System.currentTimeMillis()
    val future = operations(id)
    if (future.isDone) {
      JobCompleted
    } else {
      try {
        JobResult(id, future.get(1, TimeUnit.SECONDS))
      } catch {
        case NonFatal(e) =>
          JobFailed(id)
      }
    }
  }

  private def performOperation(m: (LocalClient => concurrent.Future[_])): Long = {
    _lastActivity = System.currentTimeMillis()
      val future = m(client.asInstanceOf[LocalClient])
      val opId = operationCounter.incrementAndGet()
      operations(opId) = future
      opId
  }

  override def id: Int = sessionId

  override def stop(): Future[Unit] = {
    Future {
      sessionState = SessionState.ShuttingDown()
      client.stop()
      sessionState = SessionState.Dead()
    }
  }

  // TODO: Add support for this in RSC
  override def logLines(): IndexedSeq[String] = {
    null
  }

  override def lastActivity: Option[Long] = Some(_lastActivity)

  override def timeout: Int = _timeout

  override def state: SessionState = sessionState
}
