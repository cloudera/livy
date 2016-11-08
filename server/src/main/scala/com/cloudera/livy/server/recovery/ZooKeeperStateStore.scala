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

import java.io.IOException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.UnhandledErrorListener
import org.apache.curator.framework.recipes.atomic.{DistributedAtomicLong => DistributedLong}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.KeeperException.NoNodeException

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.LivyConf.Entry
import com.cloudera.livy.server.batch.BatchRecoveryMetadata

object ZooKeeperStateStore {
  val ZK_KEY_PREFIX_CONF = Entry("livy.server.recovery.zk-state-store.key-prefix", "livy")
  val ZK_RETRY_CONF = Entry("livy.server.recovery.zk-state-store.retry-policy", "5,100")
}

class ZooKeeperStateStore(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None) // For testing
  extends StateStore(livyConf) with PathChildrenCacheListener with Logging {

  import ZooKeeperStateStore._

  // Constructor defined for StateStore factory to new this class using reflection.
  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val zkAddress = livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL)
  require(!zkAddress.isEmpty, s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL.key}.")
  private val zkKeyPrefix = livyConf.get(ZK_KEY_PREFIX_CONF)
  private val curatorClient = mockCuratorClient.getOrElse {
    val retryValue = livyConf.get(ZK_RETRY_CONF)
    val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
    val retryPolicy = retryValue match {
      case retryPattern(n, sleepMs) => new RetryNTimes(5, 100)
      case _ => throw new IllegalArgumentException(
        s"$ZK_KEY_PREFIX_CONF contains bad value: $retryValue. " +
          "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
    }

    CuratorFrameworkFactory.newClient(zkAddress, retryPolicy)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      curatorClient.close()
    }
  }))

  curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener {
    def unhandledError(message: String, e: Throwable): Unit = {
      error(s"Fatal Zookeeper error. Shutting down Livy server.")
      System.exit(1)
    }
  })
  curatorClient.start()

  val batchSessionsCache = new PathChildrenCache(curatorClient, prefixKey("v1/batch"), true)

  batchSessionsCache.getListenable.addListener(this)

  try {
    batchSessionsCache.start(StartMode.BUILD_INITIAL_CACHE)
  } catch {
    case _ : NullPointerException =>
      throw new IllegalArgumentException("Invalid Zookeeper settings.")
  }

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
    try {
      curatorClient.delete().guaranteed().forPath(prefixKey(key))
    } catch {
      case _: NoNodeException =>
    }
  }

  private def prefixKey(key: String) = s"/$zkKeyPrefix/$key"


  /**
    * Increment the distributed value and return the value before increment.
    * If the increment operation fails, retry until we succeed or run out of retries.
    *
    * @param distributedLong
    * The distributed long value.
    * @param retryCount
    * the remaining retry counts
    * @return
    * `Some(Long)` if we succeed otherwise `None`
    */
  @tailrec
  private def recursiveTry(distributedLong: DistributedLong, retryCount: Int): Option[Long] = {
    val updatedValue = distributedLong.increment
    updatedValue.succeeded match {
      case _ if retryCount <= 0 =>
        None
      case true if retryCount > 0 =>
        Option(updatedValue.preValue())
      case _ =>
        recursiveTry(distributedLong, retryCount - 1)
    }
  }

  /**
    *
    * @return
    */
  def nextBatchSessionId: Int = {
    val retryPolicy: RetryPolicy = new RetryNTimes(2, 3)

    val zkPath = s"/$zkKeyPrefix/batchSessionId"
    val distributedSessionId = new DistributedLong(curatorClient, zkPath, retryPolicy)

    recursiveTry(distributedSessionId, 10) match {
      case Some(sessionId) =>
        sessionId.toInt
      case None =>
        val msg: String = "Failed to get the next session id from Zookeeper"
        logger.warn(msg)
        throw new IOException(msg)
    }
  }

  override def register(batchRecoveryMetadata: BatchRecoveryMetadata): Unit = {
    sessionManagerListener.register(batchRecoveryMetadata)
  }

  override def remove(batchMetadata: BatchRecoveryMetadata): Unit = {
    sessionManagerListener.remove(batchMetadata)
  }

  override def childEvent(curator: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
    event.getType match {
      case PathChildrenCacheEvent.Type.CHILD_ADDED =>
        logger.info(s"Type.CHILD_ADDED => ${event.getData.getPath}")
        if (isNewSessionPath(event)) {
          // it is an update to a session.
          val batchMetadata = deserialize[BatchRecoveryMetadata](event.getData().getData)
          register(batchMetadata)
        } else {
          // it is an update to something else
        }

      case PathChildrenCacheEvent.Type.CHILD_REMOVED if isNewSessionPath(event) =>
        val batchMetadata = deserialize[BatchRecoveryMetadata](event.getData().getData)
        val msg = s"${event.getData.getPath}: ${event.getData.getData.map(_.toChar).mkString}"
        logger.info(s"Type.CHILD_REMOVED => ${msg}")
        remove(batchMetadata)

      case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        logger.info(s"Type.CHILD_UPDATED => ${event.getData.getPath}")
        val batchSessionData = deserialize[BatchRecoveryMetadata](event.getData().getData)
      //        register(batchSessionData)

      case PathChildrenCacheEvent.Type.CONNECTION_LOST =>
        logger.info(s"Type.CONNECTION_LOST=> ${event.getData.getPath}")

      case PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED =>
        logger.info(s"Type.CONNECTION_RECONNECTED => ${event.getData.getPath}")

      case PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED =>
        logger.info(s"Type.CONNECTION_SUSPENDED => ${event.getData.getPath}")

      case PathChildrenCacheEvent.Type.INITIALIZED =>
        logger.info(s"Type.INITIALIZED => ${event.getData.getPath}")

    }
  }

  private def isNewSessionPath(event: PathChildrenCacheEvent): Boolean = {
    event.getData.getPath.last.isDigit
  }
}
