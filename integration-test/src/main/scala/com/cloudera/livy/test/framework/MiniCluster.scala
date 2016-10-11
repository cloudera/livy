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

import java.io._
import javax.servlet.http.HttpServletResponse

import scala.concurrent.duration._
import scala.language.postfixOps

import com.ning.http.client.AsyncHttpClient
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.Logging
import com.cloudera.livy.client.common.TestUtils

private class MiniClusterConfig(val config: Map[String, String]) {

  val nmCount = getInt("yarn.nm-count", 1)
  val localDirCount = getInt("yarn.local-dir-count", 1)
  val logDirCount = getInt("yarn.log-dir-count", 1)
  val dnCount = getInt("hdfs.dn-count", 1)

  private def getInt(key: String, default: Int): Int = {
    config.get(key).map(_.toInt).getOrElse(default)
  }

}

sealed trait MiniClusterUtils extends ClusterUtils {

  protected def saveConfig(conf: Configuration, dest: File): Unit = {
    val redacted = new Configuration(conf)
    // This setting references a test class that is not available when using a real Spark
    // installation, so remove it from client configs.
    redacted.unset("net.topology.node.switch.mapping.impl")

    val out = new FileOutputStream(dest)
    try {
      redacted.writeXml(out)
    } finally {
      out.close()
    }
  }

}

sealed abstract class MiniClusterBase extends MiniClusterUtils with Logging {

  def main(args: Array[String]): Unit = {
    val klass = getClass().getSimpleName()

    info(s"$klass is starting up.")

    val Array(configPath) = args
    val config = {
      val file = new File(s"$configPath/cluster.conf")
      val props = loadProperties(file)
      new MiniClusterConfig(props)
    }
    start(config, configPath)

    info(s"$klass running.")

    while (true) synchronized {
      wait()
    }
  }

  protected def start(config: MiniClusterConfig, configPath: String): Unit

}

object MiniHdfsMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val hadoopConf = new Configuration()
    val hdfsCluster = new MiniDFSCluster.Builder(hadoopConf)
      .numDataNodes(config.dnCount)
      .format(true)
      .waitSafeMode(true)
      .build()

    hdfsCluster.waitActive()

    saveConfig(hadoopConf, new File(configPath + "/core-site.xml"))
  }

}

object MiniYarnMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val baseConfig = new YarnConfiguration()
    val yarnCluster = new MiniYARNCluster(getClass().getName(), config.nmCount,
      config.localDirCount, config.logDirCount)
    yarnCluster.init(baseConfig)

    // Install a shutdown hook for stop the service and kill all running applications.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = yarnCluster.stop()
    })

    yarnCluster.start()

    // Workaround for YARN-2642.
    val yarnConfig = yarnCluster.getConfig()
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(yarnConfig.get(YarnConfiguration.RM_ADDRESS).split(":")(1) != "0",
        "RM not up yes.")
    }

    info(s"RM address in configuration is ${yarnConfig.get(YarnConfiguration.RM_ADDRESS)}")
    saveConfig(yarnConfig, new File(configPath + "/yarn-site.xml"))
  }

}

private case class ProcessInfo(process: Process, logFile: File)

/**
 * Cluster implementation that uses HDFS / YARN mini clusters running as sub-processes locally.
 * Launching Livy through this mini cluster results in three child processes:
 *
 * - A HDFS mini cluster
 * - A YARN mini cluster
 * - The Livy server
 *
 * Each service will write its client configuration to a temporary directory managed by the
 * framework, so that applications can connect to the services.
 *
 * TODO: add support for MiniKdc.
 */
class MiniCluster(config: Map[String, String]) extends Cluster with MiniClusterUtils with Logging {

  private val tempDir = new File(sys.props("java.io.tmpdir"))
  private var sparkConfDir: File = _
  private var _configDir: File = _
  private var hdfs: Option[ProcessInfo] = None
  private var yarn: Option[ProcessInfo] = None
  private var livy: Option[ProcessInfo] = None
  private var livyUrl: String = _
  private var _hdfsScrathDir: Path = _

  override def configDir(): File = _configDir

  override def hdfsScratchDir(): Path = _hdfsScrathDir

  override def isRealSpark(): Boolean = {
    new File(sys.env("SPARK_HOME") + File.separator + "RELEASE").isFile()
  }

  override def hasSparkR(): Boolean = {
    val path = Seq(sys.env("SPARK_HOME"), "R", "lib", "sparkr.zip").mkString(File.separator)
    new File(path).isFile()
  }

  override def doAsClusterUser[T](task: => T): T = task

  // Explicitly remove the "test-lib" dependency from the classpath of child processes. We
  // want tests to explicitly upload this jar when necessary, to test those code paths.
  private val childClasspath = {
    val cp = sys.props("java.class.path").split(File.pathSeparator)
    val filtered = cp.filter { path => !new File(path).getName().startsWith("livy-test-lib-") }
    assert(cp.size != filtered.size, "livy-test-lib jar not found in classpath!")
    filtered.mkString(File.pathSeparator)
  }

  private val livyClasspath = {
    val path =
      new File(sys.env("LIVY_HOME"), s"server${File.separator}target${File.separator}jars")
    val livyJarsDir = Option(path)
      .filter(_.isDirectory())
      .getOrElse(throw new IllegalStateException(s"${path} is not a directory"))
    val livyJars = livyJarsDir.listFiles()
    require(livyJars.nonEmpty, s"${path} is empty, you need to run mvn package before running " +
      s"integration test.")

    livyJars.map(_.getAbsolutePath).mkString(File.pathSeparator)
  }

  override def deploy(): Unit = {
    sparkConfDir = mkdir("spark-conf")

    // When running a real Spark cluster, don't set the classpath.
    val extraCp = if (!isRealSpark()) {
      Map(
        SparkLauncher.DRIVER_EXTRA_CLASSPATH -> childClasspath,
        SparkLauncher.EXECUTOR_EXTRA_CLASSPATH -> childClasspath)
    } else {
      Map()
    }

    val sparkConf = extraCp ++ Map(
      "spark.executor.instances" -> "1",
      "spark.scheduler.minRegisteredResourcesRatio" -> "0.0",
      "spark.ui.enabled" -> "false",
      SparkLauncher.DRIVER_MEMORY -> "512m",
      SparkLauncher.EXECUTOR_MEMORY -> "512m",
      SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console",
      SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console"
    )
    saveProperties(sparkConf, new File(sparkConfDir, "spark-defaults.conf"))

    _configDir = mkdir("hadoop-conf")
    saveProperties(config, new File(configDir, "cluster.conf"))
    hdfs = Some(
      start(MiniHdfsMain.getClass.getName, new File(configDir, "core-site.xml"), childClasspath))
    yarn = Some(
      start(MiniYarnMain.getClass.getName, new File(configDir, "yarn-site.xml"), childClasspath))
    runLivy()

    _hdfsScrathDir = fs.makeQualified(new Path("/"))
  }

  override def cleanUp(): Unit = {
    Seq(hdfs, yarn, livy).flatten.foreach(stop)
    hdfs = None
    yarn = None
    livy = None
  }

  def runLivy(): Unit = {
    assert(!livy.isDefined)
    val confFile = new File(configDir, "serverUrl.conf")

    val log4jFile =
      Thread.currentThread().getContextClassLoader.getResource("log4j.properties").toURI
    val javaArgs = Option(TestUtils.getJacocoArgs())
      .map { args =>
        Seq(args, s"-Djacoco.args=$args")
      }.getOrElse(Nil) ++
      Seq(s"-Drepl.jars.version=${sys.props("repl.jars.version")}",
          s"-Dlog4j.configuration=${log4jFile.toString}")

    val localLivy =
      start("com.cloudera.livy.server.MiniLivyMain$", confFile, livyClasspath,
        extraJavaArgs = javaArgs)

    val props = loadProperties(confFile)
    livyUrl = props("livy.server.serverUrl")

    // Wait until Livy server responds.
    val httpClient = new AsyncHttpClient()
    eventually(timeout(30 seconds), interval(1 second)) {
      val res = httpClient.prepareGet(livyUrl).execute().get()
      assert(res.getStatusCode() == HttpServletResponse.SC_OK)
    }

    livy = Some(localLivy)
  }

  def stopLivy(): Unit = {
    assert(livy.isDefined)
    livy.foreach(stop)
    livyUrl = null
  }

  def livyEndpoint: String = livyUrl

  private def mkdir(name: String, parent: File = tempDir): File = {
    val dir = new File(parent, name)
    if (dir.isDirectory) {
      FileUtils.deleteQuietly(dir)
    }
    assert(dir.mkdir(), "Failed to create directory.")
    dir
  }

  private def start(
      klass: String,
      configFile: File,
      classpath: String,
      extraJavaArgs: Seq[String] = Nil): ProcessInfo = {
    val simpleName = klass.substring(klass.lastIndexOf(".") + 1).stripSuffix("$")
    val procDir = mkdir(simpleName)
    val procTmp = mkdir("tmp", parent = procDir)

    // Before starting anything, clean up previous running sessions.
    sys.process.Process(s"pkill -f $simpleName") !

    val java = sys.props("java.home") + "/bin/java"
    val cmd =
      Seq(
        sys.props("java.home") + "/bin/java",
        "-Dtest.appender=console",
        "-Djava.io.tmpdir=" + procTmp.getAbsolutePath(),
        "-cp", classpath + File.pathSeparator + configDir.getAbsolutePath(),
        "-XX:MaxPermSize=256m") ++
      extraJavaArgs ++
      Seq(
        klass.stripSuffix("$"),
        configDir.getAbsolutePath())

    val logFile = new File(procDir, "output.log")
    val pb = new ProcessBuilder(cmd.toArray: _*)
      .directory(procDir)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))

    pb.environment().put("LIVY_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("HADOOP_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("SPARK_CONF_DIR", sparkConfDir.getAbsolutePath())
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1")

    val child = pb.start()

    // Wait for the config file to show up before returning, so that dependent services
    // can see the configuration. Exit early if process dies.
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(configFile.isFile(), s"$simpleName hasn't started yet.")

      try {
        val exitCode = child.exitValue()
        throw new IOException(s"Child process exited unexpectedly (exit code $exitCode)")
      } catch {
        case _: IllegalThreadStateException => // Try again.
      }
    }

    ProcessInfo(child, logFile)
  }

  private def stop(svc: ProcessInfo): Unit = {
    svc.process.destroy()
    svc.process.waitFor()
  }

}
