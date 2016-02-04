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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;
import org.apache.spark.api.java.JavaFutureAction;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.MetricsCollection;
import com.cloudera.livy.client.local.BypassJobStatus;
import com.cloudera.livy.metrics.Metrics;

class BypassJobWrapper extends JobWrapper<byte[]> {

  private volatile byte[] result;
  private volatile Throwable error;
  private volatile MetricsCollection metrics;
  private volatile JobHandle.State state;
  private volatile List<Integer> newSparkJobs;

  BypassJobWrapper(RemoteDriver driver, String jobId, byte[] serializedJob) {
    super(driver, jobId, new BypassJob(driver.serializer, serializedJob));
    state = JobHandle.State.QUEUED;
  }

  @Override
  public Void call() throws Exception {
    state = JobHandle.State.STARTED;
    return super.call();
  }

  @Override
  protected synchronized void finished(byte[] result, Throwable error) {
    if (error == null) {
      this.result = result;
      this.state = JobHandle.State.SUCCEEDED;
    } else {
      this.error = error;
      this.state = JobHandle.State.FAILED;
    }
  }

  @Override
  void updateMetrics(int sparkJobId, int stageId, long taskId, Metrics metrics) {
    if (this.metrics == null) {
      synchronized (this) {
        if (this.metrics == null) {
          this.metrics = new MetricsCollection();
        }
      }
    }
    this.metrics.addMetrics(sparkJobId, stageId, taskId, metrics);
  }

  @Override
  boolean cancel() {
    if (super.cancel()) {
      this.state = JobHandle.State.CANCELLED;
      return true;
    }
    return false;
  }

  @Override
  synchronized void recordNewJob(int sparkJobId) {
    if (newSparkJobs == null) {
      newSparkJobs = new ArrayList<>();
    }
    newSparkJobs.add(sparkJobId);
  }

  @Override
  protected synchronized void jobSubmitted(JavaFutureAction<?> job) {
    for (Integer i : job.jobIds()) {
      recordNewJob(i);
    }
  }

  @Override
  protected void jobStarted() {
    // Do nothing; just avoid sending data back to the driver.
  }

  synchronized BypassJobStatus getStatus() {
    String stackTrace = error != null ? Throwables.getStackTraceAsString(error) : null;
    List<Integer> jobs = newSparkJobs;
    newSparkJobs = null;
    return new BypassJobStatus(state, result, stackTrace, metrics, jobs);
  }

}
