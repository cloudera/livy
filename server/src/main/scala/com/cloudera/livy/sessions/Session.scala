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

package com.cloudera.livy.sessions

import java.io.InputStream
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import com.cloudera.livy.{LivyConf, Logging}

abstract class Session(val id: Int, val owner: String, val livyConf: LivyConf) extends Logging {

  protected implicit val executionContext = ExecutionContext.global

  private var _lastActivity = System.nanoTime()

  // Directory where the session's staging files are created. The directory is only accessible
  // to the session's effective user.
  private var stagingDir: Path = null

  def lastActivity: Long = state match {
    case SessionState.Error(time) => time
    case SessionState.Dead(time) => time
    case SessionState.Success(time) => time
    case _ => _lastActivity
  }

  val timeout: Long = TimeUnit.HOURS.toNanos(1)

  def state: SessionState

  def stop(): Future[Unit] = Future {
    try {
      stopSession()
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

  def recordActivity(): Unit = {
    _lastActivity = System.nanoTime()
  }

  def logLines(): IndexedSeq[String]

  protected def stopSession(): Unit

  protected val proxyUser: Option[String]

  protected def doAsOwner[T](fn: => T): T = {
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

  protected def copyResourceToHDFS(dataStream: InputStream, name: String): URI = doAsOwner {
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

}
