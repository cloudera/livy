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
import java.security.PrivilegedExceptionAction
import java.util.{HashMap => JHashMap, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import com.cloudera.livy.{LivyClientBuilder, LivyConf, Logging}
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.client.local.{LocalClient, LocalConf}
import com.cloudera.livy.sessions.{Session, SessionState}

class ClientSession(
      id: Int,
      owner: String,
      val proxyUser: Option[String],
      createRequest: CreateClientRequest,
      livyConf: LivyConf)
    extends Session(id, owner) with Logging {
  implicit val executionContext = ExecutionContext.global

  var sessionState: SessionState = SessionState.Starting()

  override val timeout = TimeUnit.MILLISECONDS.toNanos(createRequest.timeout)

  private val client = {
    info(s"Creating LivyClient for sessionId: $id")
    val builder = new LivyClientBuilder()
      .setConf("spark.app.name", s"livy-session-$id")
      .setConf("spark.master", "yarn-cluster")
      .setAll(Option(createRequest.conf).getOrElse(new JHashMap()))
      .setURI(new URI("local:spark"))
      .setConf("livy.client.sessionId", id.toString)
      .setConf(LocalConf.Entry.DRIVER_CLASS.key(), null)

    proxyUser.foreach(builder.setConf(LocalConf.Entry.PROXY_USER.key(), _))
    builder.build()
  }.asInstanceOf[LocalClient]

  // Directory where the session's staging files are created. The directory is only accessible
  // to the session's effective user.
  private var stagingDir: Path = null

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
    recordActivity()
    client.addFile(uri).get()
  }

  def addJar(uri: URI): Unit = {
    recordActivity()
    client.addJar(uri).get()
  }

  def jobStatus(id: Long): Any = {
    val clientJobId = operations(id)
    recordActivity()
    // TODO: don't block indefinitely?
    val status = client.getBypassJobStatus(clientJobId).get()
    new JobStatus(id, status.state, status.result, status.error)
  }

  def cancelJob(id: Long): Unit = {
    recordActivity()
    operations.remove(id).foreach { client.cancel }
  }

  private def doAsOwner[T](fn: => T): T = {
    val user = proxyUser.getOrElse(owner)
    if (user != null) {
      val ugi = if (UserGroupInformation.isSecurityEnabled) {
        UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser())
      } else {
        UserGroupInformation.createRemoteUser(user)
      }
      ugi.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = fn
      })
    } else {
      fn
    }
  }

  private def copyResourceToHDFS(dataStream: InputStream, name: String): URI = doAsOwner {
    val fs = FileSystem.newInstance(new Configuration())

    try {
      val filePath = new Path(getStagingDir(fs), name)
      debug(s"Uploading user file to $filePath")

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
    } finally {
      fs.close()
    }
  }

  private def getStagingDir(fs: FileSystem): Path = synchronized {
    if (stagingDir == null) {
      val stagingRoot = Option(livyConf.get(LivyConf.SESSION_STAGING_DIR)).getOrElse {
        new Path(fs.getHomeDirectory(), ".livy-sessions").toString()
      }

      val sessionDir = new Path(stagingRoot, UUID.randomUUID().toString())
      fs.mkdirs(sessionDir, new FsPermission("700"))
      stagingDir = sessionDir
      debug(s"Session $id staging directory is $stagingDir")
    }
    stagingDir
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
      try {
        sessionState = SessionState.ShuttingDown()
        client.stop(true)
        sessionState = SessionState.Dead()
      } catch {
        case e: Exception =>
          warn(s"Error stopping session $id.", e)
      }

      try {
        if (stagingDir != null) {
          debug(s"Deleting session $id staging directory $stagingDir")
          doAsOwner {
            val fs = FileSystem.newInstance(new Configuration())
            try {
              fs.delete(stagingDir, true)
            } finally {
              fs.close()
            }
          }
        }
      } catch {
        case e: Exception =>
          warn(s"Error cleaning up session $id staging dir.", e)
      }
    }
  }

  // TODO: Add support for this in RSC
  override def logLines(): IndexedSeq[String] = {
    null
  }

  override def state: SessionState = sessionState
}
