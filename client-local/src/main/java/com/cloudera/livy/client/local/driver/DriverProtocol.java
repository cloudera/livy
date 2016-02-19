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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.client.local.BaseProtocol;
import com.cloudera.livy.client.local.BypassJobStatus;
import com.cloudera.livy.client.local.rpc.Rpc;

class DriverProtocol extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(DriverProtocol.class);

  private final Object jcLock;
  private final RemoteDriver driver;
  private final List<BypassJobWrapper> bypassJobs;

  DriverProtocol(RemoteDriver driver, Object jcLock) {
    this.driver = driver;
    this.jcLock = jcLock;
    this.bypassJobs = Lists.newArrayList();
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

  private void handle(ChannelHandlerContext ctx, BypassJobRequest msg) throws Exception {
    LOG.info("Received bypass job request {}", msg.id);
    BypassJobWrapper wrapper = new BypassJobWrapper(this.driver, msg.id, msg.serializedJob);
    bypassJobs.add(wrapper);
    driver.activeJobs.put(msg.id, wrapper);
    if (msg.synchronous) {
      waitForJobContext();
      try {
        wrapper.call();
      } catch (Throwable t) {
        // Wrapper already logged and saved the exception, just avoid it bubbling up
        // to the RPC layer.
      }
    } else {
      driver.submit(wrapper);
    }
  }

  @SuppressWarnings("unchecked")
  private Object handle(ChannelHandlerContext ctx, SyncJobRequest msg) throws Exception {
    waitForJobContext();
    driver.jc.setMonitorCb(new MonitorCallback() {
      @Override
      public void call(JavaFutureAction<?> future) {
        throw new IllegalStateException(
          "JobContext.monitor() is not available for synchronous jobs.");
      }
    });
    try {
      return msg.job.call(driver.jc);
    } finally {
      driver.jc.setMonitorCb(null);
    }
  }

  private BypassJobStatus handle(ChannelHandlerContext ctx, GetBypassJobStatus msg) {
    for (Iterator<BypassJobWrapper> it = bypassJobs.iterator(); it.hasNext();) {
      BypassJobWrapper job = it.next();
      if (job.jobId.equals(msg.id)) {
        BypassJobStatus status = job.getStatus();
        switch (status.state) {
          case CANCELLED:
          case FAILED:
          case SUCCEEDED:
            it.remove();
            break;

          default:
            // No-op.
        }
        return status;
      }
    }

    throw new NoSuchElementException(msg.id);
  }

  private void waitForJobContext() throws InterruptedException {
    // Wait until initialization finishes.
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
  }

}
