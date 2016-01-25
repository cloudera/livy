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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.client.local.LocalConf;
import com.cloudera.livy.client.local.rpc.Rpc;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

/**
 * Driver code for the Spark client library.
 */
public class RemoteDriver {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDriver.class);

  private final Object jcLock;
  private final Object shutdownLock;
  private final ExecutorService executor;
  private final NioEventLoopGroup egroup;
  // a local temp dir specific to this driver
  private final File localTmpDir;

  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue = Lists.newLinkedList();

  final Map<String, JobWrapper<?>> activeJobs;
  final DriverProtocol protocol;
  final Rpc clientRpc;
  final Serializer serializer;

  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  volatile JobContextImpl jc;
  volatile boolean running;

  private RemoteDriver(String[] args) throws Exception {
    this.activeJobs = Maps.newConcurrentMap();
    this.jcLock = new Object();
    this.shutdownLock = new Object();
    localTmpDir = Files.createTempDir();

    SparkConf conf = new SparkConf();
    LocalConf livyConf = new LocalConf(null);
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
      } else if (key.equals("--config-file")) {
        String path = getArg(args, idx);
        Properties p = new Properties();
        Reader r = new InputStreamReader(new FileInputStream(path), UTF_8);
        try {
          p.load(r);
        } finally {
          r.close();
        }

        for (String k : p.stringPropertyNames()) {
          livyConf.set(k, p.getProperty(k));
        }
      } else {
        throw new IllegalArgumentException("Invalid command line: "
          + Joiner.on(" ").join(args));
      }
    }

    executor = Executors.newCachedThreadPool();

    LOG.info("Connecting to: {}:{}", serverAddress, serverPort);

    String clientId = livyConf.get(CLIENT_ID);
    Preconditions.checkArgument(clientId != null, "No client ID provided.");
    String secret = livyConf.get(CLIENT_SECRET);
    Preconditions.checkArgument(secret != null, "No secret provided.");

    System.out.println("MAPCONF-->");
    System.out.println(livyConf);
    this.egroup = new NioEventLoopGroup(
        livyConf.getInt(RPC_MAX_THREADS),
        new ThreadFactoryBuilder()
            .setNameFormat("Driver-RPC-Handler-%d")
            .setDaemon(true)
            .build());
    this.serializer = new Serializer();
    this.protocol = new DriverProtocol(this, jcLock);

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

    try {
      long t1 = System.currentTimeMillis();
      LOG.info("Starting Spark context at {}", t1);
      JavaSparkContext sc = new JavaSparkContext(conf);
      LOG.info("Spark context finished initialization in {}ms", System.currentTimeMillis() - t1);
      sc.sc().addSparkListener(new DriverSparkListener(this));
      synchronized (jcLock) {
        jc = new JobContextImpl(sc, localTmpDir);
        jcLock.notifyAll();
      }
    } catch (Exception e) {
      LOG.error("Failed to start SparkContext: " + e, e);
      shutdown(e);
      synchronized (jcLock) {
        jcLock.notifyAll();
      }
      throw e;
    }

    synchronized (jcLock) {
      for (JobWrapper<?> job : jobQueue) {
        job.submit(executor);
      }
      jobQueue.clear();
    }
  }

  private void run() throws InterruptedException {
    synchronized (shutdownLock) {
      try {
        while (running) {
          shutdownLock.wait();
        }
      } catch (InterruptedException ie) {
        // Nothing to do.
      }
    }
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(localTmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
    }
  }

  void submit(JobWrapper<?> job) {
    synchronized (jcLock) {
      if (jc != null) {
        job.submit(executor);
      } else {
        LOG.info("SparkContext not yet up, queueing job request.");
        jobQueue.add(job);
      }
    }
  }

  synchronized void shutdown(Throwable error) {
    if (running) {
      if (error == null) {
        LOG.info("Shutting down remote driver.");
      } else {
        LOG.error("Shutting down remote driver due to error: " + error, error);
      }
      running = false;
      for (JobWrapper<?> job : activeJobs.values()) {
        job.cancel();
      }
      if (error != null) {
        protocol.sendError(error);
      }
      if (jc != null) {
        jc.stop();
      }
      clientRpc.close();
      egroup.shutdownGracefully();
      synchronized (shutdownLock) {
        shutdownLock.notifyAll();
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

  public static void main(String[] args) throws Exception {
    new RemoteDriver(args).run();
  }

}

