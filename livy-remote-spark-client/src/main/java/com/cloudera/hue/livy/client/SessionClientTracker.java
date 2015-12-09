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
package com.cloudera.hue.livy.client;

import com.cloudera.hue.livy.client.conf.RscConf;
import com.google.common.cache.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This class is used by the Livy servlet to get access to the SparkClient instance for a
 * specific session. Once the client is available, the job can directly be submitted to the client.
 */
public class SessionClientTracker {

  private static final Logger LOG = LoggerFactory.getLogger(SessionClientTracker.class);

  private final Object clientLock = new Object();
  private final LoadingCache<String, SparkClient> sessions =
    CacheBuilder.newBuilder()
      .expireAfterAccess(20, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<String, SparkClient>() {
        @Override
        public void onRemoval(RemovalNotification<String, SparkClient> notification) {
          synchronized (clientLock) {
            String msg = "Client for sessionId " + notification.getKey() + " is being closed";
            if (notification.getCause() == RemovalCause.EXPIRED) {
              msg = msg + " as the session has timed out";
            }
            LOG.info(msg);
            SparkClient client = notification.getValue();
            if (client != null) {
              client.stop();
              LOG.info("Successfully stopped client for sessionId: " + notification.getKey());
            }
          }
        }
      })
      .build(new CacheLoader<String, SparkClient>() {
        @Override
        public SparkClient load(String sessionId) throws Exception {
          final SparkConf sc = new SparkConf(true)
            .set("livy.client.sessionId", sessionId);
          final Map<String, String> sparkConf = new HashMap<>();
          for (Tuple2<String, String> conf : sc.getAll()) {
            sparkConf.put(conf._1(), conf._2());
          }
          LOG.info("Creating SparkClient for sessionId: " + sessionId);
          SparkClient client = SparkClientFactory.createClient(sparkConf, new RscConf());
          sessions.put(sessionId, client);
          LOG.info("Started SparkClient for sessionId: " + sessionId);
          return client;
        }
      });
  private static SessionClientTracker instance;

  @SuppressWarnings("unchecked")
  private SessionClientTracker() throws IOException {
    SparkClientFactory.initialize(Collections.EMPTY_MAP);
    LOG.info("Initialized Spark Client Factory");
  }

  /**
   * Get the SessionClientTracker singleton
   * @return
   * @throws IOException
   */
  public static synchronized SessionClientTracker get() throws IOException {
    if (instance == null) {
      instance = new SessionClientTracker();
    }
    return instance;
  }

  /**
   * Get a client to connect to submit jobs for a specific session. If the current client has
   * been closed or if the session has been closed due to a timeout, then a new Client instance
   * is created.
   * @param sessionId
   * @return
   * @throws IOException
   * @throws SparkException
   */
  public SparkClient getClient(String sessionId)
    throws IOException, SparkException {
    try {
      return sessions.get(sessionId);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause != null && cause instanceof SparkException) {
        throw (SparkException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  public void heartbeat(String sessionId) {
    try {
      LOG.debug("Received heartbeat for sessionId: " + sessionId);
      getClient(sessionId);
    } catch (Exception ex) {
      LOG.warn("Error while updating heartbeat for sessionId: " + sessionId);
    }
  }

  public void closeSession(String sessionId) {
    LOG.info("Closing session for sessionId: " + sessionId);
    sessions.invalidate(sessionId);
  }
}
