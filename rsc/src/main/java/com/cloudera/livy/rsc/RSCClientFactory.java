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

package com.cloudera.livy.rsc;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientFactory;
import com.cloudera.livy.rsc.rpc.RpcServer;

/**
 * Factory for RSC clients.
 */
public final class RSCClientFactory implements LivyClientFactory {

  private final AtomicInteger refCount = new AtomicInteger();
  private RpcServer server = null;

  /**
   * Creates a local Livy client if the URI has the "local" scheme.
   * <p>
   * If the URI contains user information, host and port, the library will try to connect to an
   * existing RSC instance with the provided information, and most of the provided configuration
   * will be ignored.
   * <p>
   * Otherwise, a new Spark context will be started with the given configuration.
   */
  @Override
  public LivyClient createClient(URI uri, Properties config) {
    if (!"local".equals(uri.getScheme())) {
      return null;
    }

    RSCConf lconf = new RSCConf(config);

    boolean needsServer = false;
    try {
      ContextInfo info;
      if (uri.getUserInfo() != null && uri.getHost() != null && uri.getPort() > 0) {
        info = createContextInfo(uri);
      } else {
        needsServer = true;
        ref(lconf);
        info = new ContextLauncher(this, lconf);
      }
      return new RSCClient(this, lconf, info);
    } catch (Exception e) {
      if (needsServer) {
        unref();
      }
      throw Utils.propagate(e);
    }
  }

  RpcServer getServer() {
    return server;
  }

  private synchronized void ref(RSCConf config) throws IOException {
    if (refCount.get() != 0) {
      refCount.incrementAndGet();
      return;
    }

    Utils.checkState(server == null, "Server already running but ref count is 0.");
    if (server == null) {
      try {
        server = new RpcServer(config);
      } catch (InterruptedException ie) {
        throw Utils.propagate(ie);
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

  private static ContextInfo createContextInfo(final URI uri) {
    final String[] userInfo = uri.getUserInfo().split(":", 2);
    return new ContextInfo() {

      @Override
      public String getRemoteAddress() {
        return uri.getHost();
      }

      @Override
      public int getRemotePort() {
        return uri.getPort();
      }

      @Override
      public String getClientId() {
        return userInfo[0];
      }

      @Override
      public String getSecret() {
        return userInfo[1];
      }

      @Override
      public void dispose(boolean forceKill) {

      }

    };
  }

}
