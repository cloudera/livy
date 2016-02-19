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

package com.cloudera.livy.server.client

import java.io.InputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import com.cloudera.livy.{LivyClientBuilder, Logging}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.client.local.LocalClient
import com.cloudera.livy.sessions.{Session, SessionState}

class ClientSession(id: Int, owner: String, createRequest: CreateClientRequest, livyHome: String)
    extends Session(id, owner) with Logging {
  implicit val executionContext = ExecutionContext.global

  var sessionState: SessionState = SessionState.Starting()

  override val timeout = TimeUnit.MILLISECONDS.toNanos(createRequest.timeout)

  private val client = {
    info(s"Creating LivyClient for sessionId: $id")
    new LivyClientBuilder()
      .setConf("spark.app.name", s"livy-session-$id")
      .setConf("spark.master", "yarn-cluster")
      .setAll(Option(createRequest.conf).getOrElse(new JHashMap()))
      .setURI(new URI("local:spark"))
      .setConf("livy.client.sessionId", id.toString)
      .build()
  }.asInstanceOf[LocalClient]

  private val fs = FileSystem.get(new Configuration())

  // TODO: It is important that each session's home be readable only by the user that created
  // that session and not by anyone else. Else, one session might be able to read files uploaded
  // by another. Fix this when we add security support.
  private val sessionHome = new Path(livyHome + "/" + id.toString)

  info("Livy client created.")

  sessionState = SessionState.Running()

  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)

  def runJob(job: Array[Byte]): Long = {
    performOperation(job, true)
  }

  def submitJob(job: Array[Byte]): Long = {
    performOperation(job, false)
  }

  def addFile(fileStream: InputStream, fileName: String): Unit = {
    addFile(copyResourceToHDFS(fileStream, fileName))
  }

  def addJar(jarStream: InputStream, jarName: String): Unit = {
    addJar(copyResourceToHDFS(jarStream, jarName))
  }

  def addFile(uri: URI): Unit = {
    client.addFile(uri).get()
  }

  def addJar(uri: URI): Unit = {
    client.addJar(uri).get()
  }

  def jobStatus(id: Long): Any = {
    val clientJobId = operations(id)
    // TODO: don't block indefinitely?
    val status = client.getBypassJobStatus(clientJobId).get()
    new JobStatus(id, status.state, status.result, status.error, status.newSparkJobs)
  }

  def cancel(id: Long): Unit = {
    operations.remove(id).foreach { client.cancel }
  }

  private def copyResourceToHDFS(dataStream: InputStream, name: String): URI = {
    val filePath = new Path(sessionHome, name)
    val outFile = fs.create(filePath, true)
    val buffer = new Array[Byte](512 * 1024)
    var read = -1
    try {
      while ({read = dataStream.read(buffer); read != -1}) {
        outFile.write(buffer, 0, read)
      }
    } finally {
      outFile.close()
    }
    filePath.toUri
  }

  private def performOperation(job: Array[Byte], sync: Boolean): Long = {
    recordActivity()
    val future = client.bypass(ByteBuffer.wrap(job), sync)
    val opId = operationCounter.incrementAndGet()
    operations(opId) = future
    opId
   }

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

  override def state: SessionState = sessionState
}
