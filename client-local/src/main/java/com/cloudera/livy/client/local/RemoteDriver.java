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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.io.FileUtils;
import org.apache.spark.JavaSparkListener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.local.rpc.Rpc;
import com.cloudera.livy.metrics.Metrics;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

/**
 * Driver code for the Spark client library.
 */
public class RemoteDriver {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDriver.class);

  private final Map<String, JobWrapper<?>> activeJobs;
  private final Object jcLock;
  private final Object shutdownLock;
  private final ExecutorService executor;
  private final NioEventLoopGroup egroup;
  private final Rpc clientRpc;
  private final DriverProtocol protocol;
  // a local temp dir specific to this driver
  private final File localTmpDir;

  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue = Lists.newLinkedList();

  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  private volatile JobContextImpl jc;
  private volatile boolean running;

  private RemoteDriver(String[] args) throws Exception {
    this.activeJobs = Maps.newConcurrentMap();
    this.jcLock = new Object();
    this.shutdownLock = new Object();
    localTmpDir = Files.createTempDir();

    SparkConf conf = new SparkConf();
    String serverAddress = null;
    int serverPort = -1;
    for (int idx = 0; idx < args.length; idx += 2) {
      String key = args[idx];
      if (key.equals("--remote-host")) {
        serverAddress = getArg(args, idx);
      } else if (key.equals("--remote-port")) {
        serverPort = Integer.parseInt(getArg(args, idx));
      } else if (key.equals("--client-id")) {
        conf.set(LocalConf.SPARK_CONF_PREFIX + CLIENT_ID.key, getArg(args, idx));
      } else if (key.equals("--secret")) {
        conf.set(LocalConf.SPARK_CONF_PREFIX + CLIENT_SECRET.key, getArg(args, idx));
      } else if (key.equals("--conf")) {
        String[] val = getArg(args, idx).split("[=]", 2);
        conf.set(val[0], val[1]);
      } else {
        throw new IllegalArgumentException("Invalid command line: "
          + Joiner.on(" ").join(args));
      }
    }

    executor = Executors.newCachedThreadPool();

    LOG.info("Connecting to: {}:{}", serverAddress, serverPort);

    LocalConf livyConf = new LocalConf(null);
    for (Tuple2<String, String> e : conf.getAll()) {
      if (e._1().startsWith(LocalConf.SPARK_CONF_PREFIX)) {
        String key = e._1().substring(LocalConf.SPARK_CONF_PREFIX.length());
        livyConf.set(key, e._2());
        LOG.debug("Remote Driver config: {} = {}", key, e._2());
      }
    }

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
    this.protocol = new DriverProtocol();

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
      sc.sc().addSparkListener(new ClientListener());
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
        job.submit();
      }
      jobQueue.clear();
    }
  }

  private void run() throws InterruptedException {
    synchronized (shutdownLock) {
      while (running) {
        shutdownLock.wait();
      }
    }
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(localTmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
    }
  }

  private void submit(JobWrapper<?> job) {
    synchronized (jcLock) {
      if (jc != null) {
        job.submit();
      } else {
        LOG.info("SparkContext not yet up, queueing job request.");
        jobQueue.add(job);
      }
    }
  }

  private synchronized void shutdown(Throwable error) {
    if (running) {
      if (error == null) {
        LOG.info("Shutting down remote driver.");
      } else {
        LOG.error("Shutting down remote driver due to error: " + error, error);
      }
      running = false;
      for (JobWrapper<?> job : activeJobs.values()) {
        cancelJob(job);
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

  private boolean cancelJob(JobWrapper<?> job) {
    boolean cancelled = false;
    for (JavaFutureAction<?> action : job.jobs) {
      cancelled |= action.cancel(true);
    }
    return cancelled | (job.future != null && job.future.cancel(true));
  }

  private String getArg(String[] args, int keyIdx) {
    int valIdx = keyIdx + 1;
    if (args.length <= valIdx) {
      throw new IllegalArgumentException("Invalid command line: "
        + Joiner.on(" ").join(args));
    }
    return args[valIdx];
  }

  private class DriverProtocol extends BaseProtocol {

    void sendError(Throwable error) {
      LOG.debug("Send error to Client: {}", Throwables.getStackTraceAsString(error));
      clientRpc.call(new java.lang.Error(error));
    }

    <T extends Serializable> void jobFinished(String jobId, T result, Throwable error) {
      LOG.debug("Send job({}) result to Client.", jobId);
      clientRpc.call(new JobResult<T>(jobId, result, error));
    }

    void jobStarted(String jobId) {
      clientRpc.call(new JobStarted(jobId));
    }

    void jobSubmitted(String jobId, int sparkJobId) {
      LOG.debug("Send job({}/{}) submitted to Client.", jobId, sparkJobId);
      clientRpc.call(new JobSubmitted(jobId, sparkJobId));
    }

    void sendMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
      LOG.debug("Send task({}/{}/{}/{}) metric to Client.", jobId, sparkJobId, stageId, taskId);
      clientRpc.call(new JobMetrics(jobId, sparkJobId, stageId, taskId, metrics));
    }

    private void handle(ChannelHandlerContext ctx, CancelJob msg) {
      JobWrapper<?> job = activeJobs.get(msg.id);
      if (job == null || !cancelJob(job)) {
        LOG.info("Requested to cancel an already finished job.");
      }
    }

    private void handle(ChannelHandlerContext ctx, EndSession msg) {
      LOG.debug("Shutting down due to EndSession request.");
      shutdown(null);
    }

    private void handle(ChannelHandlerContext ctx, JobRequest<?> msg) {
      LOG.info("Received job request {}", msg.id);
      JobWrapper<?> wrapper = new JobWrapper<>(msg);
      activeJobs.put(msg.id, wrapper);
      submit(wrapper);
    }

    private Object handle(ChannelHandlerContext ctx, SyncJobRequest msg) throws Exception {
      // In case the job context is not up yet, let's wait, since this is supposed to be a
      // "synchronous" RPC.
      if (jc == null) {
        synchronized (jcLock) {
          while (jc == null) {
            jcLock.wait();
            if (!running) {
              throw new IllegalStateException("Remote context is shutting down.");
            }
          }
        }
      }

      jc.setMonitorCb(new MonitorCallback() {
        @Override
        public void call(JavaFutureAction<?> future) {
          throw new IllegalStateException(
            "JobContext.monitor() is not available for synchronous jobs.");
        }
      });
      try {
        return msg.job.call(jc);
      } finally {
        jc.setMonitorCb(null);
      }
    }

  }

  private class JobWrapper<T extends Serializable> implements Callable<Void> {

    private final BaseProtocol.JobRequest<T> req;
    private final List<JavaFutureAction<?>> jobs;
    private final AtomicInteger completed;

    private Future<?> future;

    JobWrapper(BaseProtocol.JobRequest<T> req) {
      this.req = req;
      this.jobs = Lists.newArrayList();
      this.completed = new AtomicInteger();
    }

    @Override
    public Void call() throws Exception {
      protocol.jobStarted(req.id);

      try {
        jc.setMonitorCb(new MonitorCallback() {
          @Override
          public void call(JavaFutureAction<?> future) {
            monitorJob(future);
          }
        });

        T result = req.job.call(jc);
        synchronized (completed) {
          while (completed.get() < jobs.size()) {
            LOG.debug("Client job {} finished, {} of {} Spark jobs finished.",
                req.id, completed.get(), jobs.size());
            completed.wait();
          }
        }

        // make sure job has really succeeded
        // at this point, future.get shall not block us
        for (JavaFutureAction<?> future : jobs) {
          future.get();
        }
        protocol.jobFinished(req.id, result, null);
      } catch (Throwable t) {
        // Catch throwables in a best-effort to report job status back to the client. It's
        // re-thrown so that the executor can destroy the affected thread (or the JVM can
        // die or whatever would happen if the throwable bubbled up).
        LOG.info("Failed to run job " + req.id, t);
        protocol.jobFinished(req.id, null, t);
        throw new ExecutionException(t);
      } finally {
        jc.setMonitorCb(null);
        activeJobs.remove(req.id);
      }
      return null;
    }

    void submit() {
      this.future = executor.submit(this);
    }

    void jobDone() {
      synchronized (completed) {
        completed.incrementAndGet();
        completed.notifyAll();
      }
    }

    private void monitorJob(JavaFutureAction<?> job) {
      jobs.add(job);
      protocol.jobSubmitted(req.id, job.jobIds().get(0));
    }

  }

  private class ClientListener extends JavaSparkListener {

    private final Map<Integer, Integer> stageToJobId = Maps.newHashMap();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      synchronized (stageToJobId) {
        for (int i = 0; i < jobStart.stageIds().length(); i++) {
          stageToJobId.put((Integer) jobStart.stageIds().apply(i), jobStart.jobId());
        }
      }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      synchronized (stageToJobId) {
        for (Iterator<Map.Entry<Integer, Integer>> it = stageToJobId.entrySet().iterator();
            it.hasNext();) {
          Map.Entry<Integer, Integer> e = it.next();
          if (e.getValue() == jobEnd.jobId()) {
            it.remove();
          }
        }
      }

      String clientId = getClientId(jobEnd.jobId());
      if (clientId != null) {
        activeJobs.get(clientId).jobDone();
      }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.reason() instanceof org.apache.spark.Success$
          && !taskEnd.taskInfo().speculative()) {
        Metrics metrics = new Metrics(taskEnd.taskMetrics());
        Integer jobId;
        synchronized (stageToJobId) {
          jobId = stageToJobId.get(taskEnd.stageId());
        }

        // TODO: implement implicit AsyncRDDActions conversion instead of jc.monitor()?
        // TODO: how to handle stage failures?

        String clientId = getClientId(jobId);
        if (clientId != null) {
          protocol.sendMetrics(clientId, jobId, taskEnd.stageId(),
            taskEnd.taskInfo().taskId(), metrics);
        }
      }
    }

    /**
     * Returns the client job ID for the given Spark job ID.
     *
     * This will only work for jobs monitored via JobContext#monitor(). Other jobs won't be
     * matched, and this method will return `None`.
     */
    private String getClientId(Integer jobId) {
      for (Map.Entry<String, JobWrapper<?>> e : activeJobs.entrySet()) {
        for (JavaFutureAction<?> future : e.getValue().jobs) {
          if (future.jobIds().contains(jobId)) {
            return e.getKey();
          }
        }
      }
      return null;
    }

  }

  public static void main(String[] args) throws Exception {
    new RemoteDriver(args).run();
  }

}

