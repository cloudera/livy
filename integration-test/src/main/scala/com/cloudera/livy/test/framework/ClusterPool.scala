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

import com.cloudera.livy.Logging

trait ClusterPool {
  def init(): Unit
  def destroy(): Unit
  def lease(): Cluster
  def returnCluster(cluster: Cluster): Unit
}

object ClusterPool extends Logging {
  private val CLUSTER_TYPE = "cluster.type"

  private val config = {
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

  private val clusterPool =
    try {
      val pool = config.get(CLUSTER_TYPE) match {
        case Some("real") => new RealClusterPool(config)
        case Some("mini") => new MiniClusterPool(config)
        case t => throw new Exception(s"Unknown or unset cluster.type $t")
      }
      pool.init()
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          pool.destroy()
        }
      })
      pool
    } catch {
      case e: Throwable =>
        error("Failed to initialize cluster.", e)
        throw e
    }

  def get: ClusterPool = {
    clusterPool
  }
}
