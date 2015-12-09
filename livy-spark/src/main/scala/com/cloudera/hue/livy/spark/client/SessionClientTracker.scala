/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.cloudera.hue.livy.spark.client

import com.cloudera.hue.livy.Logging
import com.cloudera.hue.livy.client.SparkClient
import com.cloudera.hue.livy.client.SparkClientFactory
import com.cloudera.hue.livy.client.conf.RscConf
import com.google.common.cache._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import java.io.IOException
import java.util.{Collections, Map}
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

/**
  * This class is used by the Livy servlet to get access to the SparkClient instance for a
  * specific session. Once the client is available, the job can directly be submitted to the client.
  */
object SessionClientTracker extends Logging {
  SparkClientFactory.initialize(Collections.emptyMap())
  info("Initialized Spark Client Factory")

  private val clientLock: AnyRef = new AnyRef
  private val sessions: Cache[Integer, SparkClient] = CacheBuilder.newBuilder
    .expireAfterAccess(20, TimeUnit.MINUTES)
    .removalListener(new RemovalListener[Integer, SparkClient]() {
      def onRemoval(notification: RemovalNotification[Integer, SparkClient]) {
        clientLock synchronized {
          var msg: String = "Client for sessionId " + notification.getKey + " is being closed"
          if (notification.getCause eq RemovalCause.EXPIRED) {
            msg = msg + " as the session has timed out"
          }
          SessionClientTracker.info(msg)
          val client: SparkClient = notification.getValue
          if (client != null) {
            client.stop()
            info("Successfully stopped client for sessionId: " + notification.getKey)
          }
        }
      }
    }).build()

  @throws(classOf[IOException])
  @throws(classOf[SparkException])
  def createClient(sessionId: Integer, sparkConf: Map[String, String]): SparkClient = {
    val sc = new SparkConf(true)
    for (conf <- sc.getAll) {
      sparkConf.put(conf._1, conf._2)
    }
    sparkConf.put("livy.client.sessionId", sessionId.toString)
    sparkConf.put("spark.master", "yarn-cluster")
    info("Creating SparkClient for sessionId: " + sessionId)
    val client: SparkClient = SparkClientFactory.createClient(sparkConf, new RscConf)
    sessions.put(sessionId, client)
    info("Started SparkClient for sessionId: " + sessionId)
    client
  }

  /**
    * Get a client to connect to submit jobs for a specific session. If the current client has
    * been closed or if the session has been closed due to a timeout, then a new Client instance
    * is created.
    * @param sessionId
    * @return The SparkClient for the given session, None if the session has been closed.
    */
  @throws(classOf[Exception])
  def getClient(sessionId: Integer): Option[SparkClient] = {
    Option(sessions.getIfPresent(sessionId))
  }

  def heartbeat(sessionId: Integer) {
    try {
      debug("Received heartbeat for sessionId: " + sessionId)
      getClient(sessionId)
    }
    catch {
      case NonFatal(e) =>
        warn("Error while updating heartbeat for sessionId: " + sessionId)
    }
  }

  def closeSession(sessionId: Integer) {
    info("Closing session for sessionId: " + sessionId)
    sessions.invalidate(sessionId)
  }
}
