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

import com.google.common.base.Throwables;
import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.client.local.BaseProtocol;
import com.cloudera.livy.client.local.rpc.Rpc;
import com.cloudera.livy.metrics.Metrics;

class DriverProtocol extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(DriverProtocol.class);

  private final Object jcLock;
  private final RemoteDriver driver;

  DriverProtocol(RemoteDriver driver, Object jcLock) {
    this.driver = driver;
    this.jcLock = jcLock;
  }

  void sendError(Throwable error) {
    LOG.debug("Send error to Client: {}", Throwables.getStackTraceAsString(error));
    driver.clientRpc.call(new java.lang.Error(error));
  }

  <T> void jobFinished(String jobId, T result, Throwable error) {
    LOG.debug("Send job({}) result to Client.", jobId);
    driver.clientRpc.call(new JobResult<T>(jobId, result, error));
  }

  void jobStarted(String jobId) {
    driver.clientRpc.call(new JobStarted(jobId));
  }

  void jobSubmitted(String jobId, int sparkJobId) {
    LOG.debug("Send job({}/{}) submitted to Client.", jobId, sparkJobId);
    driver.clientRpc.call(new JobSubmitted(jobId, sparkJobId));
  }

  void sendMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
    LOG.debug("Send task({}/{}/{}/{}) metric to Client.", jobId, sparkJobId, stageId, taskId);
    driver.clientRpc.call(new JobMetrics(jobId, sparkJobId, stageId, taskId, metrics));
  }

  private void handle(ChannelHandlerContext ctx, CancelJob msg) {
    JobWrapper<?> job = driver.activeJobs.get(msg.id);
    if (job == null || !job.cancel()) {
      LOG.info("Requested to cancel an already finished job.");
    }
  }

  private void handle(ChannelHandlerContext ctx, EndSession msg) {
    LOG.debug("Shutting down due to EndSession request.");
    driver.shutdown(null);
  }

  private void handle(ChannelHandlerContext ctx, JobRequest<?> msg) {
    LOG.info("Received job request {}", msg.id);
    JobWrapper<?> wrapper = new JobWrapper<>(this.driver, msg.id, msg.job);
    driver.activeJobs.put(msg.id, wrapper);
    driver.submit(wrapper);
  }

  private void handle(ChannelHandlerContext ctx, BypassJobRequest msg) {
    LOG.info("Received bypass job request {}", msg.id);
    JobWrapper<?> wrapper = new JobWrapper<>(this.driver, msg.id,
      new BypassJob(driver.serializer, msg.serializedJob));
    driver.activeJobs.put(msg.id, wrapper);
    driver.submit(wrapper);
  }

  @SuppressWarnings("unchecked")
  private Object handle(ChannelHandlerContext ctx, SyncJobRequest msg) throws Exception {
    return runSyncJob(msg.job);
  }

  private byte[] handle(ChannelHandlerContext ctx, BypassSyncJob msg) throws Exception {
    Job<byte[]> job = new BypassJob(driver.serializer, msg.serializedJob);
    return runSyncJob(job);
  }

  private <T> T runSyncJob(Job<T> job) throws Exception {
    // In case the job context is not up yet, let's wait, since this is supposed to be a
    // "synchronous" RPC.
    if (driver.jc == null) {
      synchronized (jcLock) {
        while (driver.jc == null) {
          jcLock.wait();
          if (!driver.running) {
            throw new IllegalStateException("Remote context is shutting down.");
          }
        }
      }
    }

    driver.jc.setMonitorCb(new MonitorCallback() {
      @Override
      public void call(JavaFutureAction<?> future) {
        throw new IllegalStateException(
          "JobContext.monitor() is not available for synchronous jobs.");
      }
    });
    try {
      return job.call(driver.jc);
    } finally {
      driver.jc.setMonitorCb(null);
    }
  }

}
