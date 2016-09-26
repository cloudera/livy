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
package com.cloudera.livy.server.recovery

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.LivyConf.Entry

object ZooKeeperStateStore extends StateStoreCompanion {
  val ZK_KEY_PREFIX_CONF = Entry("livy.server.recovery.zk-state-store.key-prefix", "livy")

  override def create(livyConf: LivyConf): StateStore = new ZooKeeperStateStore(livyConf)
}

class ZooKeeperStateStore(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None) // For testing
  extends StateStore with Logging {
  private val zkAddress = livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL_CONF)
  require(!zkAddress.isEmpty, s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL_CONF.key}.")
  private val zkKeyPrefix = livyConf.get(ZooKeeperStateStore.ZK_KEY_PREFIX_CONF)
  private val curatorClient = mockCuratorClient.getOrElse {
    CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(5, 100))
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      curatorClient.close()
    }
  }))

  curatorClient.start()
  // TODO Make sure ZK path has proper secure permissions so that other users cannot read its
  // contents.

  override def set(key: String, value: Object): Unit = {
    val prefixedKey = prefixKey(key)
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(prefixedKey, data)
    } else {
      curatorClient.setData().forPath(prefixedKey, data)
    }
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      None
    } else {
      Option(deserialize[T](curatorClient.getData().forPath(prefixedKey)))
    }
  }

  override def getChildren(key: String): Seq[String] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      Seq.empty[String]
    } else {
      curatorClient.getChildren.forPath(prefixedKey).asScala
    }
  }

  override def remove(key: String): Unit = {
    curatorClient.delete().guaranteed().forPath(prefixKey(key))
  }

  private def prefixKey(key: String) = s"/$zkKeyPrefix/$key"
}
