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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.local.rpc.Rpc;

/**
 * Driver code for the Spark client library.
 */
public class RemoteDriver extends Driver {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDriver.class);

  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue = Lists.newLinkedList();

  protected final ExecutorService executor;
  // a local temp dir specific to this driver
  private final File localTmpDir;

  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  volatile JobContextImpl jc;

  private RemoteDriver(String[] args) throws Exception {
    super(args);
    localTmpDir = Files.createTempDir();
    executor = Executors.newCachedThreadPool();
    try {
      long t1 = System.nanoTime();
      LOG.info("Starting Spark context...");
      JavaSparkContext sc = new JavaSparkContext(conf);
      LOG.info("Spark context finished initialization in {}ms",
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1));
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

  @Override
  public DriverProtocol createProtocol(Rpc client) {
    return new DriverProtocol(this, client, jcLock);
  }

  @Override
  public synchronized void shutdown(Throwable error) {
    if (!running) {
      return;
    }

    try {
      if (error == null) {
        LOG.info("Shutting down remote driver.");
      } else {
        LOG.error("Shutting down remote driver due to error: " + error, error);
      }
      for (JobWrapper<?> job : activeJobs.values()) {
        job.cancel();
      }
      if (jc != null) {
        jc.stop();
      }
      stopClients(error);

      executor.shutdownNow();
      try {
        FileUtils.deleteDirectory(localTmpDir);
      } catch (IOException e) {
        LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
      }
    } finally {
      running = false;
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

  @Override
  public void setMonitorCallback(MonitorCallback cb) {
    jc.setMonitorCb(cb);
  }


  public static void main(String[] args) throws Exception {
    new RemoteDriver(args).run();
  }

}

