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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.Promise;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.client.common.AbstractJobHandle;

/**
 * A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 */
class JobHandleImpl<T> extends AbstractJobHandle<T> {

  private final RSCClient client;
  private final String jobId;
  private final Promise<T> promise;
  private volatile State state;

  JobHandleImpl(RSCClient client, Promise<T> promise, String jobId) {
    this.client = client;
    this.jobId = jobId;
    this.promise = promise;
  }

  /** Requests a running job to be cancelled. */
  @Override
  public boolean cancel(boolean mayInterrupt) {
    if (changeState(State.CANCELLED)) {
      client.cancel(jobId);
      promise.cancel(mayInterrupt);
      return true;
    }
    return false;
  }

  @Override
  public T get() throws ExecutionException, InterruptedException {
    return promise.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    return promise.get(timeout, unit);
  }

  @Override
  public boolean isCancelled() {
    return promise.isCancelled();
  }

  @Override
  public boolean isDone() {
    return promise.isDone();
  }

  @Override
  protected T result() {
    return promise.getNow();
  }

  @Override
  protected Throwable error() {
    return promise.cause();
  }

  @SuppressWarnings("unchecked")
  void setSuccess(Object result) {
    // The synchronization here is not necessary, but tests depend on it.
    synchronized (listeners) {
      promise.setSuccess((T) result);
      changeState(State.SUCCEEDED);
    }
  }

  void setFailure(Throwable error) {
    // The synchronization here is not necessary, but tests depend on it.
    synchronized (listeners) {
      promise.setFailure(error);
      changeState(State.FAILED);
    }
  }

}
