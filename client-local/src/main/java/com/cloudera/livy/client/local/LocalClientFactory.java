/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.client.local;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.spark.SparkException;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientFactory;
import com.cloudera.livy.client.local.rpc.RpcServer;

/**
 * Factory for local Livy clients.
 */
public final class LocalClientFactory implements LivyClientFactory {

  private final AtomicInteger refCount = new AtomicInteger();
  private RpcServer server = null;

  /**
   * Creates a local Livy client if the URI has the "local" scheme. Any other components of the
   * URI are ignored.
   */
  @Override
  public LivyClient createClient(URI uri, Properties config) {
    if (!"local".equals(uri.getScheme())) {
      return null;
    }

    LocalConf lconf = new LocalConf(config);
    try {
      ref(lconf);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }

    try {
      return new LocalClient(this, lconf);
    } catch (Exception e) {
      unref();
      throw Throwables.propagate(e);
    }
  }

  RpcServer getServer() {
    return server;
  }

  private synchronized void ref(LocalConf config) throws IOException {
    if (refCount.get() != 0) {
      refCount.incrementAndGet();
      return;
    }

    Preconditions.checkState(server == null);
    if (server == null) {
      try {
        server = new RpcServer(config);
      } catch (InterruptedException ie) {
        throw Throwables.propagate(ie);
      }
    }

    refCount.incrementAndGet();
  }

  synchronized void unref() {
    if (refCount.decrementAndGet() == 0) {
      server.close();
      server = null;
    }
  }

}
