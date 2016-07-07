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

import java.io.{File, IOException}
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import javax.servlet.http.HttpServletResponse._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

import com.decodified.scalassh._
import com.ning.http.client.AsyncHttpClient
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{LivyConf, Logging}

private class RealClusterConfig(config: Map[String, String]) {
  val ip = config("ip")

  val sshLogin = config("ssh.login")
  val sshPubKey = config("ssh.pubkey")
  val livyPort = config.getOrElse(LivyConf.SERVER_PORT.key, "8998").toInt
  val livyClasspath = config.getOrElse("livy.classpath", "")

  val deployLivy = config.getOrElse("deploy-livy", "true").toBoolean
  val noDeployLivyHome = config.get("livy-home")

  val sparkHome = config("env.spark_home")
  val sparkConf = config.getOrElse("env.spark_conf", "/etc/spark/conf")
  val hadoopConf = config.getOrElse("env.hadoop_conf", "/etc/hadoop/conf")

  val javaHome = config.getOrElse("env.java_home", "/usr/java/default")
}

class RealCluster(_config: Map[String, String])
  extends Cluster with ClusterUtils with Logging {

  private val config = new RealClusterConfig(_config)

  private var livyIsRunning = false
  private var livyHomePath: String = _
  private var livyEpoch = 0

  private var _configDir: File = _

  private var hdfsScratch: Path = _

  private var sparkConfDir: String = _
  private var tempDirPath: String = _
  private var _hasSparkR: Boolean = _

  override def isRealSpark(): Boolean = true

  override def hasSparkR(): Boolean = _hasSparkR

  override def configDir(): File = _configDir

  override def hdfsScratchDir(): Path = hdfsScratch

  override def doAsClusterUser[T](task: => T): T = {
    val user = UserGroupInformation.createRemoteUser(config.sshLogin)
    user.doAs(new PrivilegedExceptionAction[T] {
      override def run(): T = task
    })
  }

  private def sshClient[T](body: SshClient => SSH.Result[T]): T = {
    val sshLogin = PublicKeyLogin(
      config.sshLogin, None, config.sshPubKey :: Nil)
    val hostConfig = HostConfig(login = sshLogin, hostKeyVerifier = HostKeyVerifiers.DontVerify)
    SSH(config.ip, hostConfig)(body) match {
      case Left(err) => throw new IOException(err)
      case Right(result) => result
    }
  }

  private def exec(cmd: String): CommandResult = {
    info(s"Running command: $cmd")
    val result = sshClient(_.exec(cmd))
    result.exitCode match {
      case Some(ec) if ec > 0 =>
        throw new IOException(s"Command '$cmd' failed: $ec\n" +
          s"stdout: ${result.stdOutAsString()}\n" +
          s"stderr: ${result.stdErrAsString()}\n")
      case _ =>
    }
    result
  }

  private def upload(local: String, remote: String): Unit = {
    info(s"Uploading local path $local")
    sshClient(_.upload(local, remote))
  }

  private def download(remote: String, local: String): Unit = {
    info(s"Downloading remote path $remote")
    sshClient(_.download(remote, local))
  }

  override def deploy(): Unit = {
    // Make sure Livy is not running.
    stopLivy()

    // Check whether SparkR is supported in YARN (need the sparkr.zip archive).\
    _hasSparkR = try {
      exec(s"test -f ${config.sparkHome}/R/lib/sparkr.zip")
      true
    } catch {
      case e: IOException => false
    }

    // Copy the remove Hadoop configuration to a local temp dir so that tests can use it to
    // talk to HDFS and YARN.
    val localTemp = new File(sys.props("java.io.tmpdir") + File.separator + "hadoop-conf")
    download(config.hadoopConf, localTemp.getAbsolutePath())
    _configDir = localTemp

    // Create a temp directory where test files will be written.
    tempDirPath = s"/tmp/livy-it-${Random.alphanumeric.take(16).mkString}"
    exec(s"mkdir -p $tempDirPath")

    // Also create an HDFS scratch directory for tests.
    doAsClusterUser {
      hdfsScratch = fs.makeQualified(
        new Path(s"/tmp/livy-it-${Random.alphanumeric.take(16).mkString}"))
      fs.mkdirs(hdfsScratch)
      fs.setPermission(hdfsScratch, new FsPermission("777"))
    }

    // Create a copy of the Spark configuration, and make sure the master is "yarn-cluster".
    sparkConfDir = s"$tempDirPath/spark-conf"
    val sparkProps = s"$sparkConfDir/spark-defaults.conf"
    Seq(
      s"cp -Lr ${config.sparkConf} $sparkConfDir",
      s"touch $sparkProps",
      s"sed -i.old '/spark.master.*/d' $sparkProps",
      s"sed -i.old '/spark.submit.deployMode.*/d' $sparkProps",
      s"echo 'spark.master=yarn-cluster' >> $sparkProps"
    ).foreach(exec)

    if (config.deployLivy) {
      try {
        info(s"Deploying Livy to ${config.ip}...")
        val version = sys.props("project.version")
        val assemblyZip = new File(s"../assembly/target/livy-server-$version.zip")
        assert(assemblyZip.isFile,
          s"Can't find livy assembly zip at ${assemblyZip.getCanonicalPath}")
        val assemblyName = assemblyZip.getName()

        // SSH to the node to unzip and install Livy.
        upload(assemblyZip.getCanonicalPath, s"$tempDirPath/$assemblyName")
        exec(s"unzip -o $tempDirPath/$assemblyName -d $tempDirPath")
        livyHomePath = s"$tempDirPath/livy-server-$version"
        info(s"Deployed Livy to ${config.ip} at $livyHomePath.")
      } catch {
        case e: Exception =>
          error(s"Failed to deploy Livy to ${config.ip}.", e)
          cleanUp()
          throw e
      }
    } else {
      livyHomePath = config.noDeployLivyHome.get
      info("Skipping deployment.")
    }

    runLivy()
  }

  override def cleanUp(): Unit = {
    stopLivy()
    if (tempDirPath != null) {
      exec(s"rm -rf $tempDirPath")
    }
    if (hdfsScratch != null) {
      doAsClusterUser {
        // Cannot use the shared `fs` since this runs in a shutdown hook, and that instance
        // may have been closed already.
        val fs = FileSystem.newInstance(hadoopConf)
        try {
          fs.delete(hdfsScratch, true)
        } finally {
          fs.close()
        }
      }
    }
  }

  override def runLivy(): Unit = synchronized {
    assert(!livyIsRunning, "Livy is running already.")

    val livyConf = Map(
      "livy.server.port" -> config.livyPort.toString,
      // "livy.server.recovery.mode=local",
      "livy.environment" -> "development",
      LivyConf.LIVY_SPARK_MASTER.key -> "yarn",
      LivyConf.LIVY_SPARK_DEPLOY_MODE.key -> "cluster")
    val livyConfFile = File.createTempFile("livy.", ".properties")
    saveProperties(livyConf, livyConfFile)
    upload(livyConfFile.getAbsolutePath(), s"$livyHomePath/conf/livy.conf")

    val env = Map(
        "JAVA_HOME" -> config.javaHome,
        "HADOOP_CONF_DIR" -> config.hadoopConf,
        "SPARK_CONF_DIR" -> sparkConfDir,
        "SPARK_HOME" -> config.sparkHome,
        "CLASSPATH" -> config.livyClasspath,
        "LIVY_PID_DIR" -> s"$tempDirPath/pid",
        "LIVY_LOG_DIR" -> s"$tempDirPath/logs",
        "LIVY_MAX_LOG_FILES" -> "16",
        "LIVY_IDENT_STRING" -> "it"
      )
    val livyEnvFile = File.createTempFile("livy-env.", ".sh")
    saveProperties(env, livyEnvFile)
    upload(livyEnvFile.getAbsolutePath(), s"$livyHomePath/conf/livy-env.sh")

    info(s"Starting Livy @ port ${config.livyPort}...")
    exec(s"env -i $livyHomePath/bin/livy-server start")
    livyIsRunning = true

    val httpClient = new AsyncHttpClient()
    eventually(timeout(1 minute), interval(1 second)) {
      assert(httpClient.prepareGet(livyEndpoint).execute().get().getStatusCode == SC_OK)
    }
    info(s"Started Livy.")
  }

  override def stopLivy(): Unit = synchronized {
    info("Stopping Livy Server")
    try {
      exec(s"$livyHomePath/bin/livy-server stop")
    } catch {
      case e: Exception =>
        if (livyIsRunning) {
          throw e
        }
    }

    if (livyIsRunning) {
      // Wait a tiny bit so that the process finishes closing its output files.
      Thread.sleep(2)

      livyEpoch += 1
      val logName = "livy-it-server.out"
      val localName = s"livy-it-server-$livyEpoch.out"
      val logPath = s"$tempDirPath/logs/$logName"
      val localLog = sys.props("java.io.tmpdir") + File.separator + localName
      download(logPath, localLog)
      info(s"Log for epoch $livyEpoch available at $localLog")
    }
  }

  override def livyEndpoint: String = s"http://${config.ip}:${config.livyPort}"
}

