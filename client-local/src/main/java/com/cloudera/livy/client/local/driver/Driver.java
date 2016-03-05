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
package com.cloudera.livy.client.local.driver;

import com.cloudera.livy.JobContext;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.client.local.LocalConf;
import com.cloudera.livy.client.local.rpc.Rpc;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static com.cloudera.livy.client.local.LocalConf.Entry.CLIENT_ID;
import static com.cloudera.livy.client.local.LocalConf.Entry.CLIENT_SECRET;
import static com.cloudera.livy.client.local.LocalConf.Entry.RPC_MAX_THREADS;

public abstract class Driver {
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  final NioEventLoopGroup egroup;

  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue = Lists.newLinkedList();
  final Map<String, JobWrapper<?>> activeJobs = Maps.newConcurrentMap();

  final DriverProtocol protocol;
  final Rpc clientRpc;
  final Serializer serializer;
  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  volatile JobContext jc;
  final LocalConf livyConf;
  final SparkConf conf;
  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  volatile boolean running;
  public Driver(String[] args) throws Exception {
    livyConf = new LocalConf(null);
    conf = new SparkConf();
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
        livyConf.set(CLIENT_SECRET.key(), getArg(args, idx));
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

    this.egroup = new NioEventLoopGroup(
      livyConf.getInt(RPC_MAX_THREADS),
      new ThreadFactoryBuilder()
        .setNameFormat("Driver-RPC-Handler-%d")
        .setDaemon(true)
        .build());
    this.serializer = new Serializer();
    this.protocol = new DriverProtocol(this, new Object());

    // The RPC library takes care of timing out this.
    this.clientRpc = Rpc.createClient(livyConf, egroup, serverAddress, serverPort,
      clientId, secret, protocol).get();
    this.running = true;

    this.clientRpc.addListener(new Rpc.Listener() {
      @Override
      public void rpcClosed(Rpc rpc) {
        LOG.warn("Shutting down driver because RPC channel was closed.");
        shutdown(null);
      }
    });

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

  abstract void setMonitorCallback(MonitorCallback bc);

  abstract void shutdown(Throwable error);
}
