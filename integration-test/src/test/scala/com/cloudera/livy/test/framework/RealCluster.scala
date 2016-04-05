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

package com.cloudera.livy.test.framework

import java.io.File

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

import com.decodified.scalassh._
import com.ning.http.client.AsyncHttpClient

import com.cloudera.livy.Logging

class RealCluster(
  ip: String,
  livyPort: Int,
  userName: String,
  keyFilePath: String,
  useExistingLivyServer: Boolean)
  extends Cluster with Logging {

  private var livyHomePath: Option[String] = None
  private var pathsToCleanUp = ListBuffer.empty[String]

  def sshClient[T](body: SshClient => SSH.Result[T]): Validated[T] = {
    val sshLogin = PublicKeyLogin(userName, None, keyFilePath :: Nil)
    val hostConfig = HostConfig(login = sshLogin, hostKeyVerifier = HostKeyVerifiers.DontVerify)
    SSH(ip, hostConfig)(body)
  }

  override def deploy(): Unit = {
    if (useExistingLivyServer) {
      info("cluster.real.use.existing.livy.server is true. Skipping deployment.")
    } else {
      try {
        info(s"Deploying Livy to $ip...")
        val assemblyZip = new File("../assembly/target/livy-server-0.2.0-SNAPSHOT-livy-server.zip")
        assert(assemblyZip.isFile,
          s"Can't find livy assembly zip at ${assemblyZip.getAbsolutePath}")

        // Upload Livy to /tmp/<random dir>
        val tempDirPath = s"/tmp/${Random.alphanumeric.take(16).mkString}"
        pathsToCleanUp += tempDirPath

        // SSH to the node to unzip and install Livy.
        val deployResult = for {
          _ <- sshClient(_.exec(s"mkdir -p $tempDirPath")).right
          _ <- sshClient(_.upload(assemblyZip.getAbsolutePath, s"$tempDirPath/test.zip")).right
          _ <- sshClient(_.exec(s"unzip -o $tempDirPath/test.zip -d $tempDirPath")).right
          livyHome <- findLivyHomePath(tempDirPath).right
          _ <- verifyLivyServerExists(livyHome).right
        } yield {
          livyHome
        }

        livyHomePath = deployResult match {
          case Left(err) => throw new Exception(err)
          case Right(livyHome) =>
            info(s"Livy installed @ $livyHome")
            Option(livyHome)
        }
        info(s"Deployed Livy to $ip.")
      } catch {
        case e: Exception =>
          error(s"Failed to deploy Livy to $ip.", e)
          cleanUp()
          throw e
      }
    }
  }

  override def cleanUp(): Unit = {
    if (!useExistingLivyServer) {
      pathsToCleanUp.foreach(p => sshClient(_.exec(s"rm -rf $p")))
      pathsToCleanUp.clear()
    }
  }

  override def getYarnRmEndpoint(): String = ""

  override def runLivy(): Unit = {
    if (!useExistingLivyServer) {
      val livyHome = livyHomePath.get

      val livyOpts = s"'-Dlivy.server.port=$livyPort -Dlivy.server.master=yarn " +
        "-Dlivy.server.deployMode=cluster'"
      val classPath = s"CLASSPATH=`hadoop classpath`:/usr/hdp/current/spark-client/lib/*"
      val opts = s"$classPath LIVY_SERVER_JAVA_OPTS=$livyOpts"
      val livyServerPath = s"$livyHome/bin/livy-server"

      info(s"Starting Livy @ port $livyPort...")
      val r = sshClient(_.exec(s"$opts nohup $livyServerPath > $logPath 2>&1 &"))
      r match {
        case Left(err) => throw new Exception(err)
        case Right(cr: CommandResult) =>
          cr.exitCode match {
            case Some(0) =>
              // block until Livy server is up.
              // This is really ugly. Fix this!
              val httpClient = new AsyncHttpClient()
              @tailrec
              def healthCheck(deadline: Deadline = 1.minute.fromNow): Unit = {
                try {
                  require(httpClient.prepareGet(livyEndpoint).execute().get().getStatusCode == 200)
                } catch {
                  case e: Exception =>
                    if (deadline.isOverdue()) {
                      throw new Exception("Livy server failed to start within a minute.")
                    } else {
                      Thread.sleep(1.second.toMillis)
                      healthCheck(deadline)
                    }
                }
              }
              healthCheck()
            case _ =>
              throw new Exception(s"Failed to start Livy(${cr.exitCode}). ${cr.stdErrAsString()}")
          }
      }
      info(s"Started Livy.")
    }
  }

  override def getLivyLog(): String = {
    if (useExistingLivyServer) {
      "Unable to get log if using an existing livy server."
    } else {
      sshClient(_.exec(s"cat $logPath")).right.map(_.stdOutAsString()) match {
        case Left(_) => s"Failed to get log: $logPath"
        case Right(s) => s
      }
    }
  }

  override def stopLivy(): Unit = {
    if (!useExistingLivyServer) {
      // TODO, add a force kill flag
      sshClient(_.exec(s"pkill -f com.cloudera.livy.server.Main")).right.map(_.stdOutAsString())
    }
  }

  override def livyEndpoint: String = s"http://$ip:$livyPort"

  override def upload(srcPath: String, destPath: String): Unit = {
    sshClient(_.upload(srcPath, destPath)) match {
      case Left(err) => throw new Exception(err)
      case Right(_) =>
    }
  }

  private def findLivyHomePath(tempDirPath: String): Either[String, String] = {
    sshClient(_.exec(s"ls -d $tempDirPath/*/")).right.map { r =>
      val stdOut = r.stdOutAsString().trim
      if (stdOut.isEmpty) {
        Left("ls didn't return any folders.")
      } else {
        Right(stdOut)
      }
    }.joinRight
  }

  private def logPath = s"${livyHomePath.get}/log"

  private def verifyLivyServerExists(livyHome: String): Either[String, Unit] = {
    sshClient(_.exec(s"[ -e $livyHome/bin/livy-server ]")).right.map { r =>
      r.exitCode match {
        case Some(0) => Right()
        case _ => Left(s"$livyHome/bin/livy-server doesn't exist. ${r.exitCode}")
      }
    }.joinRight
  }
}
