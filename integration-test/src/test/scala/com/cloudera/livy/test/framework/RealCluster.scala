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
import scala.sys.process._
import scala.util.Random

import com.decodified.scalassh._
import com.ning.http.client.AsyncHttpClient

import com.cloudera.livy.Logging

class RealCluster(
  ip: String,
  config: TestEnvConfig.RealClusterConfig)
  extends Cluster with Logging {

  private var livyHomePath: Option[String] = None
  private var pathsToCleanUp = ListBuffer.empty[String]

  def sshClient[T](body: SshClient => SSH.Result[T]): Validated[T] = {
    val sshLogin = PublicKeyLogin(
      config.sshLogin, None, config.sshPubKey :: Nil)
    val hostConfig = HostConfig(login = sshLogin, hostKeyVerifier = HostKeyVerifiers.DontVerify)
    SSH(ip, hostConfig)(body)
  }

  override def deploy(): Unit = {
    if (config.deployLivy) {
      try {
        def findLivyHomePath(tempDirPath: String): Either[String, String] = {
          sshClient(_.exec(s"ls -d $tempDirPath/*/")).right.map { r =>
            val stdOut = r.stdOutAsString().trim
            if (stdOut.isEmpty) {
              Left("ls didn't return any folders.")
            } else {
              Right(stdOut)
            }
          }.joinRight
        }

        def rsync(src: String, dest: String): Either[String, Unit] = {
          val rsyncOutput = new StringBuilder
          val cmd = s"rsync -avc $src ${config.sshLogin}@${config.ip}:$dest"
          val exitCode = cmd.run(ProcessLogger(rsyncOutput.append(_))).exitValue()
          if (exitCode != 0) {
            Left(s"rsync '$cmd' failed with ${rsyncOutput.toString()}")
          } else {
            Right()
          }
        }

        info(s"Deploying Livy to $ip...")
        val assemblyZip = new File("../assembly/target/livy-server-0.2.0-SNAPSHOT-livy-server.zip")
        assert(assemblyZip.isFile,
          s"Can't find livy assembly zip at ${assemblyZip.getCanonicalPath}")

        val uploadAssemblyZipPath = config.deployLivyPath.get
        // Upload Livy to /tmp/<random dir>
        val tempDirPath = s"/tmp/${Random.alphanumeric.take(16).mkString}"
        pathsToCleanUp += tempDirPath

        val livyConfPath = TestUtil.saveTestSourceToTempFile("livy-defaults.conf").getAbsolutePath

        // SSH to the node to unzip and install Livy.
        val deployResult = for {
          _ <- sshClient(_.exec(s"mkdir -p $tempDirPath")).right
          _ <- rsync(assemblyZip.getCanonicalPath, uploadAssemblyZipPath).right
          _ <- sshClient(_.exec(s"unzip -o $uploadAssemblyZipPath -d $tempDirPath")).right
          livyHome <- findLivyHomePath(tempDirPath).right
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
    } else {
      livyHomePath = config.noDeployLivyHome
      info("Skipping deployment.")
    }
  }

  override def cleanUp(): Unit = {
    if (config.deployLivy) {
      pathsToCleanUp.foreach(p => sshClient(_.exec(s"rm -rf $p")))
      pathsToCleanUp.clear()
    }
  }

  private var livyLog: String = ""

  override def getYarnRmEndpoint(): String = ""

  private val livyServerThread = new Thread {
    override def run() = {
      val livyHome = livyHomePath.get
      val livyPort = config.livyPort
      val livyJavaOptsValue = Seq(
        s"livy.server.port=$livyPort",
        "livy.server.master=yarn",
        "livy.server.deployMode=cluster",
        "livy.environment=development").map("-D" + _).mkString(" ")

      val livyJavaOpts = s"LIVY_SERVER_JAVA_OPTS='$livyJavaOptsValue'"
      val classPath = s"CLASSPATH=${config.livyClasspath}"
      val opts = s"$classPath $livyJavaOpts"
      val livyServerPath = s"$livyHome/bin/livy-server"

      info(s"Starting Livy @ port $livyPort...")
      val r = sshClient(_.exec(s"$opts $livyServerPath 2>&1"))
      r match {
        case Left(err) => throw new Exception(err)
        case Right(cr: CommandResult) =>
          livyLog = cr.stdOutAsString()
          cr.exitCode match {
            case Some(0) =>
            case _ =>
              error(cr.stdOutAsString())
              throw new Exception(s"Livy exited with code ${cr.exitCode}.")
          }
      }
    }
  }

  override def runLivy(): Unit = {
    livyServerThread.start()

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
    info(s"Started Livy.")
  }

  override def getLivyLog(): String = {
    if (config.deployLivy) {
      livyLog
    } else {
      "Unable to get log if using an existing livy server."
    }
  }

  override def stopLivy(): Unit = {
    sshClient(_.exec(s"pkill -f com.cloudera.livy.server.Main")).right.map(_.stdOutAsString())
    if (livyServerThread.isAlive) {
      livyServerThread.join()
    }
  }

  override def upload(srcPath: String, destPath: String): Unit = {
    sshClient(_.upload(srcPath, destPath)) match {
      case Left(err) => throw new Exception(err)
      case Right(_) =>
    }
  }

  override def livyEndpoint: String = s"http://$ip:${config.livyPort}"
}
