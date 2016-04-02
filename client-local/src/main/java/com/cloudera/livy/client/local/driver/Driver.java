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

package com.cloudera.livy.client.local.driver;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.JobContext;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.client.local.BaseProtocol;
import com.cloudera.livy.client.local.LocalConf;
import com.cloudera.livy.client.local.rpc.Rpc;
import com.cloudera.livy.client.local.rpc.RpcDispatcher;
import com.cloudera.livy.client.local.rpc.RpcServer;

import static com.cloudera.livy.client.local.LocalConf.Entry.*;

public abstract class Driver {
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  protected RpcServer server;
  // Keeps track of connected clients.
  private final List<DriverProtocol> clients = Lists.newArrayList();
  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue = Lists.newLinkedList();
  final Map<String, JobWrapper<?>> activeJobs = Maps.newConcurrentMap();

  protected final Serializer serializer;
  protected final SparkConf conf;
  protected final LocalConf livyConf;

  protected final Object jcLock;

  protected final Object shutdownLock;
  volatile boolean running;

  protected Driver(String[] args) throws Exception {
    conf = new SparkConf();
    livyConf = new LocalConf(null);
    jcLock = new Object();
    shutdownLock = new Object();
    for (Tuple2<String, String> e : conf.getAll()) {
      String key = e._1();
      String value = e._2();
      if (key.startsWith(LocalConf.LIVY_SPARK_PREFIX)) {
        livyConf.set(key.substring(LocalConf.LIVY_SPARK_PREFIX.length()), value);
      }
    }

    String serverAddress = null;
    int serverPort = -1;
    for (int idx = 0; idx < args.length; idx += 2) {
      String key = args[idx];
      if (key.equals("--remote-host")) {
        serverAddress = getArg(args, idx);
      } else if (key.equals("--remote-port")) {
        serverPort = Integer.parseInt(getArg(args, idx));
      } else if (key.equals("--client-id")) {
        livyConf.set(CLIENT_ID, getArg(args, idx));
      } else if (key.equals("--secret")) {
        livyConf.set(CLIENT_SECRET, getArg(args, idx));
      } else if (key.equals("--conf")) {
        String[] val = getArg(args, idx).split("[=]", 2);
        if (val[0].startsWith(LocalConf.SPARK_CONF_PREFIX)) {
          conf.set(val[0], val[1]);
        } else {
          livyConf.set(val[0], val[1]);
        }
      } else {
        throw new IllegalArgumentException("Invalid command line: "
          + Joiner.on(" ").join(args));
      }
    }

    LOG.info("Connecting to: {}:{}", serverAddress, serverPort);

    String clientId = livyConf.get(CLIENT_ID);
    Preconditions.checkArgument(clientId != null, "No client ID provided.");
    String secret = livyConf.get(CLIENT_SECRET);
    Preconditions.checkArgument(secret != null, "No secret provided.");

    this.serializer = new Serializer();

    // We need to unset this configuration since it doesn't really apply for the driver side.
    // If the driver runs on a multi-homed machine, this can lead to issues where the Livy
    // server cannot connect to the auto-detected address, but since the driver can run anywhere
    // on the cluster, it would be tricky to solve that problem in a generic way.
    livyConf.set(RPC_SERVER_ADDRESS, null);

    // Bring up the RpcServer an register the secret provided by the Livy server as a client.
    LOG.info("Starting RPC server...");
    this.server = new RpcServer(livyConf);
    server.registerClient(clientId, secret, new RpcServer.ClientCallback() {
      @Override
      public RpcDispatcher onNewClient(Rpc client) {
        final DriverProtocol dispatcher = createProtocol(client);
        synchronized (clients) {
          clients.add(dispatcher);
        }
        client.addListener(new Rpc.Listener() {
          @Override
          public void rpcClosed(Rpc rpc) {
            synchronized (clients) {
              clients.remove(dispatcher);
            }
          }
        });
        LOG.debug("Registered new connection from {}.", client.getChannel());
        return dispatcher;
      }
    });

    // The RPC library takes care of timing out this.
    Rpc callbackRpc = Rpc.createClient(livyConf, server.getEventLoopGroup(), serverAddress,
      serverPort, clientId, secret, new BaseProtocol() { }).get();
    try {
      // There's no timeout here because we expect the launching side to kill the underlying
      // application if it takes too long to connect back and send its address.
      callbackRpc.call(new BaseProtocol.RemoteDriverAddress(server.getAddress(),
        server.getPort())).get();
    } finally {
      callbackRpc.close();
    }

    this.running = true;
  }

  public final synchronized void shutdown(Throwable error) {
    if (!running) {
      return;
    }

    try {
      if (error == null) {
        LOG.info("Shutting down remote driver.");
      } else {
        LOG.error("Shutting down remote driver due to error: " + error, error);
      }
      shutdownDriver();
      stopClients(error);
    } finally {
      running = false;
      synchronized (shutdownLock) {
        shutdownLock.notifyAll();
      }
    }
  }

  public abstract void setMonitorCallback(MonitorCallback bc);

  public abstract void shutdownDriver();

  public abstract DriverProtocol createProtocol(Rpc client);

  public abstract void submit(JobWrapper<?> job);

  public abstract JobContext jobContext();

  public void run() throws InterruptedException {
    synchronized (shutdownLock) {
      try {
        while (running) {
          shutdownLock.wait();
        }
      } catch (InterruptedException ie) {
        // Nothing to do.
      }
    }
  }

  private String getArg(String[] args, int keyIdx) {
    int valIdx = keyIdx + 1;
    if (args.length <= valIdx) {
      throw new IllegalArgumentException("Invalid command line: "
        + Joiner.on(" ").join(args));
    }
    return args[valIdx];
  }

  protected LocalConf getLivyConf() {
    return livyConf;
  }

  protected SparkConf getSparkConf() {
    return conf;
  }

  protected void stopClients(Throwable error) {
    if (error != null) {
      synchronized (clients) {
        for (DriverProtocol client : clients) {
          client.sendError(error);
        }
      }
    }
    if (server != null) {
      server.close();
    }
  }
}
