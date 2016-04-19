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

import java.util.Properties

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

trait ClusterPool {
  def init(): Unit
  def destroy(): Unit
  def lease(): Cluster
  def returnCluster(cluster: Cluster): Unit
}

object TestEnvConfig {
  private val properties = {
    val p = new Properties()
    p.load(getClass.getClassLoader.getResourceAsStream("test-env.properties"))
    p.asScala
  }

  class RealClusterConfig {
    val ip: String = properties.get("real-cluster.ip").get

    val sshLogin: String = properties.get("real-cluster.ssh.login").get
    val sshPubKey: String = properties.get("real-cluster.ssh.pubkey").get
    val livyPort = properties.getOrElse("real-cluster.livy.port", "8998").toInt
    val livyClasspath = properties.get("real-cluster.livy.classpath").get

    val deployLivy = properties.getOrElse("real-cluster.deploy-livy", "true").toBoolean
    val deployLivyPath = properties.get("real-cluster.deploy-livy.path")
    val noDeployLivyHome = properties.get("real-cluster.livy.no-deploy.livy-home")
  }

  def clusterType: String = properties.getOrElse("cluster.type", "real")

  val realCluster: Option[RealClusterConfig] = clusterType match {
    case "real" => Some(new RealClusterConfig())
    case _ => None
  }
}

object ClusterPool {
  private val clusterPool = TestEnvConfig.clusterType match {
    case "real" => new RealClusterPool()
    case "mini" => new MiniClusterPool()
    case t => throw new Exception(s"Unknown cluster.type $t")
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      clusterPool.destroy()
    }
  })

  clusterPool.init()

  def get: ClusterPool = {
    clusterPool
  }
}

/**
 * Test cases will request a cluster from this class so test cases can run in parallel
 * if there are spare clusters.
 */
class RealClusterPool extends ClusterPool {
  val ipList = TestEnvConfig.realCluster.get.ip.split(",")
  val realClusterConfig = TestEnvConfig.realCluster.get

  val clusters = ipList.map(ip => new RealCluster(ip, realClusterConfig)).toBuffer

  override def init(): Unit = synchronized {
    clusters.foreach(_.deploy())
    clusters.foreach(_.stopLivy())
    clusters.foreach(_.runLivy())
  }

  override def destroy(): Unit = synchronized {
    clusters.foreach(_.stopLivy())
    clusters.foreach(_.cleanUp())
  }

  override def lease(): Cluster = synchronized {
    clusters.remove(0)
  }

  override def returnCluster(cluster: Cluster): Unit = synchronized {
    clusters.append(cluster.asInstanceOf[RealCluster])
  }
}
