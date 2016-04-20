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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.LivyServer

sealed trait MiniClusterUtils {

  protected def saveConfig(conf: Configuration, dest: File): Unit = {
    val out = new FileOutputStream(dest)
    try {
      conf.writeXml(out)
    } finally {
      out.close()
    }
  }

  protected def saveProperties(props: Properties, dest: File): Unit = {
    val tempFile = new File(dest.getAbsolutePath() + ".tmp")
    val out = new OutputStreamWriter(new FileOutputStream(tempFile), UTF_8)
    try {
      props.store(out, "Configuration")
    } finally {
      out.close()
    }
    tempFile.renameTo(dest)
  }

}

sealed abstract class MiniClusterBase extends MiniClusterUtils with Logging {

  protected val testConfig = {
    val in = new InputStreamReader(
      getClass().getResourceAsStream("/test-env.properties"), UTF_8)
    val props = new Properties()
    try {
      props.load(in)
    } finally {
      in.close()
    }
    props
  }

  protected def getIntConfig(key: String, default: Int): Int = {
    Option(testConfig.getProperty(key)).map(_.toInt).getOrElse(default)
  }

  def main(args: Array[String]): Unit = {
    val klass = getClass().getSimpleName()

    info(s"$klass is starting up.")

    val Array(configPath) = args
    start(configPath)

    info(s"$klass running.")

    while (true) synchronized {
      wait()
    }
  }

  protected def start(configPath: String): Unit

}

object MiniHdfsMain extends MiniClusterBase {

  override protected def start(configPath: String): Unit = {
    val dnCount = getIntConfig("mini-cluster.hdfs.dn-count", 1)

    val config = new Configuration()
    val hdfsCluster = new MiniDFSCluster.Builder(config)
      .numDataNodes(dnCount)
      .format(true)
      .waitSafeMode(true)
      .build()

    hdfsCluster.waitActive()

    saveConfig(config, new File(configPath + "/core-site.xml"))
  }

}

object MiniYarnMain extends MiniClusterBase {

  override protected def start(configPath: String): Unit = {
    val nmCount = getIntConfig("mini-cluster.yarn.nm-count", 1)
    val localDirCount = getIntConfig("mini-cluster.yarn.local-dir-count", 1)
    val logDirCount = getIntConfig("mini-cluster.yarn.log-dir-count", 1)

    val baseConfig = new YarnConfiguration()
    var yarnCluster = new MiniYARNCluster(getClass().getName(), nmCount, localDirCount, logDirCount)
    yarnCluster.init(baseConfig)
    yarnCluster.start()

    // Woraround for YARN-2642.
    val config = yarnCluster.getConfig()
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) != "0",
        "RM not up yes.")
    }

    info(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")
    saveConfig(config, new File(configPath + "/yarn-site.xml"))
  }

}

object MiniLivyMain extends MiniClusterBase {

  def start(configPath: String): Unit = {
    val server = new LivyServer()
    server.start()

    // Write a livy-defaults.conf file to the conf directory with the location of the Livy
    // server. Do it atomically since it's used by MiniCluster to detect when the Livy server
    // is up and ready.
    val clientConf = new Properties()
    clientConf.setProperty("livy.server.serverUrl", server.serverUrl())

    saveProperties(clientConf, new File(configPath + "/livy-defaults.conf"))
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
class MiniCluster extends Cluster with MiniClusterUtils with Logging {

  private val tempDir = new File(sys.props("java.io.tmpdir"))
  private var sparkConfDir: File = _
  private var configDir: File = _
  private var hdfs: Option[ProcessInfo] = None
  private var yarn: Option[ProcessInfo] = None
  private var livy: Option[ProcessInfo] = None
  private var livyUrl: String = _

  override def deploy(): Unit = {
    sparkConfDir = mkdir("spark-conf")

    val sparkConf = new Properties()
    sparkConf.setProperty(SparkLauncher.SPARK_MASTER, "yarn")
    sparkConf.setProperty("spark.submit.deployMode", "cluster")
    sparkConf.setProperty(SparkLauncher.DRIVER_EXTRA_CLASSPATH, sys.props("java.class.path"))
    sparkConf.setProperty(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, sys.props("java.class.path"))
    saveProperties(sparkConf, new File(sparkConfDir, "spark-defaults.conf"))

    configDir = mkdir("hadoop-conf")
    hdfs = Some(start(MiniHdfsMain.getClass, new File(configDir, "core-site.xml")))
    yarn = Some(start(MiniYarnMain.getClass, new File(configDir, "yarn-site.xml")))
    runLivy()
  }

  override def cleanUp(): Unit = {
    Seq(hdfs, yarn, livy).flatten.foreach(stop)
    hdfs = None
    yarn = None
    livy = None
  }

  // TODO?
  override def getYarnRmEndpoint: String = ""

  override def upload(srcPath: String, destPath: String): Unit = {
    // TODO?
  }

  def runLivy(): Unit = {
    assert(!livy.isDefined)
    val confFile = new File(configDir, "livy-defaults.conf")
    val localLivy = start(MiniLivyMain.getClass, confFile)

    val in = new InputStreamReader(new FileInputStream(confFile), UTF_8)
    val props = new Properties()
    try {
      props.load(in)
    } finally {
      in.close()
    }
    livyUrl = props.getProperty("livy.server.serverUrl")
    livy = Some(localLivy)
  }

  def stopLivy(): Unit = {
    assert(livy.isDefined)
    livy.foreach(stop)
    livyUrl = null
  }

  def livyEndpoint: String = livyUrl

  def getLivyLog(): String = livy.map(_.logFile.getAbsolutePath()).getOrElse("")

  override def runCommand(cmd: String): String = {
    throw new UnsupportedOperationException()
  }

  private def mkdir(name: String, parent: File = tempDir): File = {
    val dir = new File(parent, name)
    if (!dir.isDirectory()) {
      assert(dir.mkdir(), "Failed to create directory.")
    }
    dir
  }

  private def start(klass: Class[_], configFile: File): ProcessInfo = {
    val simpleName = klass.getSimpleName().stripSuffix("$")
    val procDir = mkdir(simpleName)
    val procTmp = mkdir("tmp", parent = procDir)

    val java = sys.props("java.home") + "/bin/java"
    val cmd = Seq(
      sys.props("java.home") + "/bin/java",
      "-Dtest.appender=console",
      "-Djava.io.tmpdir=" + procTmp.getAbsolutePath(),
      "-cp", sys.props("java.class.path") + File.pathSeparator + configDir.getAbsolutePath(),
      "-Xmx2g",
      klass.getName().stripSuffix("$"),
      configDir.getAbsolutePath())

    val logFile = new File(procDir, "output.log")
    val pb = new ProcessBuilder(cmd.toArray: _*)
      .directory(procDir)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))

    pb.environment().put("HADOOP_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("SPARK_CONF_DIR", sparkConfDir.getAbsolutePath())

    val child = pb.start()

    // Wait for the config file to show up before returning, so that dependent services
    // can see the configuration.
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(configFile.isFile(), s"$simpleName hasn't started yet.")
    }

    ProcessInfo(child, logFile)
  }

  private def stop(svc: ProcessInfo): Unit = {
    svc.process.destroy()
    svc.process.waitFor()
  }

}

class MiniClusterPool extends ClusterPool {

  private val cluster = new MiniCluster()

  override def init(): Unit = synchronized {
    cluster.deploy()
  }

  override def destroy(): Unit = synchronized {
    cluster.cleanUp()
  }

  override def lease(): Cluster = cluster

  override def returnCluster(cluster: Cluster): Unit = ()

}
