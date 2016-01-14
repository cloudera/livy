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

package com.cloudera.livy.spark.client

import java.util.concurrent.{TimeUnit, Callable, Executors, ScheduledFuture}

import com.cloudera.livy.LivyClient
import com.cloudera.livy.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.mutable.HashMap

class SessionCache extends Logging{

  val sessions = HashMap[Int, TimeoutTracker]()
  val lock = new Object()
  val evictionExecutor = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Session Timeout Thread").build())

  /**
    * Get the LivyClient for the given sessionId. If there is no client present, this returns None
    * @param sessionId The sessionId for which the LivyClient is to be retrieved.
    * @return LivyClient if session is active and has a Spark Client associated, else None.
    */
  def get(sessionId: Int): Option[LivyClient] = lock.synchronized {
    sessions.get(sessionId) match {
      case Some(tracker) =>
        val future = tracker.scheduledFuture
        val client = tracker.sparkClient
        val cancelled = future.cancel(false)
        // If it was cancelled successfully, then the client has not timed out. So update the
        // timeout and return the client.
        if (cancelled) {
          tracker.scheduledFuture =
            evictionExecutor.schedule(ClientCloser(client), tracker.timeout, TimeUnit.SECONDS)
          info("Updated timeout for sessionId: " + sessionId)
          Some(client)
        } else {
          warn("Session " + sessionId + " already closed. Cannot update the timeout!")
          None
        }
      case None =>
        error("No session found for sessionId: " + sessionId)
        None
    }
  }

  /**
    * Update the session's "active" time. This pushes the timeout further.
    * @param sessionId The sessionId whose timeout is to be updated
    * @return true if the timeout was successfully updated else false.
    */
  def refresh(sessionId: Int): Boolean = {
    get(sessionId).isDefined
  }

  /**
    * Put a new Spark Client for the given sessionId. If an old Spark Client exists for this
    * session, the caller must stop it to avoid a leak.
    * @param sessionId The sessionId for which the Spark Client is to be cached.
    * @param timeout Time in seconds after which the client should be closed if it is not
    *                refreshed or fetched during that interval
    * @param sparkClient The client to store.
    */
  def put(sessionId: Int, timeout: Long, sparkClient: LivyClient): Unit = lock.synchronized {
    sessions(sessionId) = new TimeoutTracker(
      sparkClient,
      timeout,
      evictionExecutor.schedule(ClientCloser(sparkClient), timeout, TimeUnit.SECONDS))
    info("Added client with id: " + sparkClient.clientId() + " for sessionId: " + sessionId)
  }

  /**
    * Remove the Spark Client associated with this sessionId
    * @param sessionId The sessionId whose Spark Client should be removed.
    */
  def remove(sessionId: Int): Unit = lock.synchronized {
    sessions.remove(sessionId) match {
      case Some(tracker) =>
        if (!tracker.scheduledFuture.isDone) {
          tracker.sparkClient.stop()
          tracker.scheduledFuture.cancel(true)
          info("Removed client for sessionId: " + sessionId)
        } else {
          warn("Attempted to stop client for sessionId: " + sessionId + " which has already been " +
            "stopped!")
        }
      case None =>
    }
  }

  /**
    * A wrapper around Spark Client that is used to track the timeout and the future that closes
    * the Client
    */
  class TimeoutTracker(
    val sparkClient: LivyClient,
    val timeout: Long,
    var scheduledFuture: ScheduledFuture[Unit]
  )

  /**
    * Simple class that closes the given client.
    * @param client
    */
  case class ClientCloser(client: LivyClient) extends Callable[Unit] {
    override def call(): Unit = lock.synchronized {
      client.stop()
    }
  }

}
