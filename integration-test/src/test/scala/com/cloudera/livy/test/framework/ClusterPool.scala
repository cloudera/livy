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

trait ClusterPool {
  def init(): Unit
  def destroy(): Unit
  def lease(): Cluster
  def returnCluster(cluster: Cluster): Unit
}

object TestEnvConfig {
  private val prop = new Properties()
  prop.load(getClass.getClassLoader.getResourceAsStream("test-env.properties"))

  def clusterType: String = prop.getProperty("cluster.type", "real")
  def realClusterIp: Option[String] = Option(prop.getProperty("cluster.real.ip"))
  def realClusterLivyPort: Int = prop.getProperty("cluster.real.livy.port", "8998").toInt
  def realClusterUseExistingLivyServer: Boolean =
    prop.getProperty("cluster.real.use.existing.livy.server", "false").toBoolean
  def realClusterSshLogin: Option[String] = Option(prop.getProperty("cluster.real.ssh.login"))
  def realClusterSshPubKey: Option[String] =
    Option(prop.getProperty("cluster.real.ssh.pubkey-path"))
}

object ClusterPool {
  private val clusterPool = TestEnvConfig.clusterType match {
    case "real" => new RealClusterPool()
    case t => throw new Exception(s"Unknown cluster.type $t")
  }
  clusterPool.init()

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      clusterPool.destroy()
    }
  })

  def get: ClusterPool = {
    clusterPool
  }
}

/**
 * Test cases will request a cluster from this class so test cases can run in parallel
 * if there are spare clusters.
 */
class RealClusterPool extends ClusterPool {
  val ip = TestEnvConfig.realClusterIp.get
  val livyPort = TestEnvConfig.realClusterLivyPort
  val sshLogin = TestEnvConfig.realClusterSshLogin.get
  val sshPubKey = TestEnvConfig.realClusterSshPubKey.get
  val useExistingLivyServer = TestEnvConfig.realClusterUseExistingLivyServer

  val clusters = List(new RealCluster(ip, livyPort, sshLogin, sshPubKey, useExistingLivyServer))

  override def init(): Unit = synchronized {
    clusters.foreach(_.deploy())
  }

  override def destroy(): Unit = synchronized {
    clusters.foreach(_.cleanUp())
  }

  override def lease(): Cluster = synchronized {
    clusters.head
  }

  override def returnCluster(cluster: Cluster): Unit = synchronized {

  }
}
