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

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.client.api.YarnClient

import com.cloudera.livy.Logging

/**
 * An common interface to run test on real cluster and mini cluster.
 */
trait Cluster {
  def deploy(): Unit
  def cleanUp(): Unit
  def configDir(): File
  def isRealSpark(): Boolean
  def hasSparkR(): Boolean

  def runLivy(): Unit
  def stopLivy(): Unit
  def livyEndpoint: String
  def hdfsScratchDir(): Path

  def doAsClusterUser[T](task: => T): T

  lazy val hadoopConf = {
    val conf = new Configuration(false)
    configDir().listFiles().foreach { f =>
      if (f.getName().endsWith(".xml")) {
        conf.addResource(new Path(f.toURI()))
      }
    }
    conf
  }

  lazy val yarnConf = {
    val conf = new Configuration(false)
    conf.addResource(new Path(s"${configDir().getCanonicalPath}/yarn-site.xml"))
    conf
  }

  lazy val fs = doAsClusterUser {
    FileSystem.get(hadoopConf)
  }

  lazy val yarnClient = doAsClusterUser {
    val c = YarnClient.createYarnClient()
    c.init(yarnConf)
    c.start()
    c
  }
}

object Cluster extends Logging {
  private val CLUSTER_TYPE = "cluster.type"

  private lazy val config = {
    sys.props.get("cluster.spec")
      .filter { path => path.nonEmpty && path != "default" }
      .map { path =>
        val in = Option(getClass.getClassLoader.getResourceAsStream(path))
          .getOrElse(new FileInputStream(path))
        val p = new Properties()
        val reader = new InputStreamReader(in, UTF_8)
        try {
          p.load(reader)
        } finally {
          reader.close()
        }
        p.asScala.toMap
      }
      .getOrElse(Map(CLUSTER_TYPE -> "mini"))
  }

  private lazy val cluster = {
    var _cluster: Cluster = null
    try {
      _cluster = config.get(CLUSTER_TYPE) match {
        case Some("real") => new RealCluster(config)
        case Some("mini") => new MiniCluster(config)
        case t => throw new Exception(s"Unknown or unset cluster.type $t")
      }
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          info("Shutting down cluster pool.")
          _cluster.cleanUp()
        }
      })
      _cluster.deploy()
    } catch {
      case e: Throwable =>
        error("Failed to initialize cluster.", e)
        Option(_cluster).foreach { c =>
          Try(c.cleanUp()).recover { case e =>
            error("Furthermore, failed to clean up cluster after failure.", e)
          }
        }
        throw e
    }
    _cluster
  }

  def get(): Cluster = cluster
}

trait ClusterUtils {

  protected def saveProperties(props: Map[String, String], dest: File): Unit = {
    val jprops = new Properties()
    props.foreach { case (k, v) => jprops.put(k, v) }

    val tempFile = new File(dest.getAbsolutePath() + ".tmp")
    val out = new OutputStreamWriter(new FileOutputStream(tempFile), UTF_8)
    try {
      jprops.store(out, "Configuration")
    } finally {
      out.close()
    }
    tempFile.renameTo(dest)
  }

  protected def loadProperties(file: File): Map[String, String] = {
    val in = new InputStreamReader(new FileInputStream(file), UTF_8)
    val props = new Properties()
    try {
      props.load(in)
    } finally {
      in.close()
    }
    props.asScala.toMap
  }

}
