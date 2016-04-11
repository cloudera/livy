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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaFutureAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;

public class JobWrapper<T> implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(JobWrapper.class);

  public final String jobId;

  private final RSCDriver driver;
  private final List<JavaFutureAction<?>> sparkJobs;
  private final Job<T> job;
  private final AtomicInteger completed;

  private Future<?> future;

  JobWrapper(RSCDriver driver, String jobId, Job<T> job) {
    this.driver = driver;
    this.jobId = jobId;
    this.job = job;
    this.sparkJobs = Lists.newArrayList();
    this.completed = new AtomicInteger();
  }

  @Override
  public Void call() throws Exception {
    try {
      jobStarted();
      T result = job.call(driver.jobContext());
      synchronized (completed) {
        while (completed.get() < sparkJobs.size()) {
          LOG.debug("Client job {} finished, {} of {} Spark jobs finished.",
              jobId, completed.get(), sparkJobs.size());
          completed.wait();
        }
      }

      // make sure job has really succeeded
      // at this point, future.get shall not block us
      for (JavaFutureAction<?> future : sparkJobs) {
        future.get();
      }
      finished(result, null);
    } catch (Throwable t) {
      // Catch throwables in a best-effort to report job status back to the client. It's
      // re-thrown so that the executor can destroy the affected thread (or the JVM can
      // die or whatever would happen if the throwable bubbled up).
      LOG.info("Failed to run job " + jobId, t);
      finished(null, t);
      throw new ExecutionException(t);
    } finally {
      driver.activeJobs.remove(jobId);
    }
    return null;
  }

  void submit(ExecutorService executor) {
    this.future = executor.submit(this);
  }

  void jobDone() {
    synchronized (completed) {
      completed.incrementAndGet();
      completed.notifyAll();
    }
  }

  boolean cancel() {
    boolean cancelled = false;
    for (JavaFutureAction<?> action : sparkJobs) {
      cancelled |= action.cancel(true);
    }
    return cancelled | (future != null && future.cancel(true));
  }

  boolean hasSparkJobId(Integer sparkId) {
    for (JavaFutureAction<?> future : sparkJobs) {
      if (future.jobIds().contains(sparkId)) {
        return true;
      }
    }
    return false;
  }

  protected void finished(T result, Throwable error) {
    if (error == null) {
      driver.jobFinished(jobId, result, null);
    } else {
      driver.jobFinished(jobId, null, error);
    }
  }

  protected void jobStarted() {
    driver.jobStarted(jobId);
  }

}
