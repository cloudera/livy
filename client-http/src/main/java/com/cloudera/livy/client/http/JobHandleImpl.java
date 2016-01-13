/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.client.http;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.client.common.AbstractJobHandle;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.common.Serializer;
import static com.cloudera.livy.client.common.HttpMessages.*;
import static com.cloudera.livy.client.http.HttpConf.Entry.*;

class JobHandleImpl<T> extends AbstractJobHandle<T> {

  private final LivyConnection conn;
  private final ScheduledExecutorService executor;
  private final Object lock;
  private final Serializer serializer;

  private final long initialPollInterval;
  private final long maxPollInterval;

  private long jobId;
  private T result;
  private Throwable error;
  private volatile boolean isDone;
  private volatile boolean isCancelled;
  private volatile ScheduledFuture<?> pollTask;

  JobHandleImpl(
      HttpConf config,
      LivyConnection conn,
      ScheduledExecutorService executor,
      Serializer s) {
    this.conn = conn;
    this.executor = executor;
    this.lock = new Object();
    this.serializer = s;
    this.isDone = false;

    this.initialPollInterval = config.getTimeAsMs(JOB_INITIAL_POLL_INTERVAL);
    this.maxPollInterval = config.getTimeAsMs(JOB_MAX_POLL_INTERVAL);

    if (initialPollInterval <= 0) {
      throw new IllegalArgumentException("Invalid initial poll interval.");
    }
    if (maxPollInterval <= 0 || maxPollInterval < initialPollInterval) {
      throw new IllegalArgumentException(
        "Invalid max poll interval, or lower than initial interval.");
    }
  }

  @Override
  public T get() throws ExecutionException, InterruptedException {
    try {
      return get(true, -1, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      // Not gonna happen.
      throw new RuntimeException(te);
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    return get(false, timeout, unit);
  }

  @Override
  public boolean isDone() {
    return isDone;
  }

  @Override
  public boolean isCancelled() {
    return isCancelled;
  }

  @Override
  public boolean cancel(boolean mayInterrupt) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected T result() {
    return result;
  }

  @Override
  protected Throwable error() {
    return error;
  }

  void start(
      final int sessionId,
      final String command,
      final ByteBuffer serializedJob) {

    Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          ClientMessage msg = new SerializedJob(BufferUtils.toByteArray(serializedJob));
          JobStatus status = conn.post(msg, JobStatus.class, "/%d/%s", sessionId, command);
          jobId = status.id;

          pollTask = executor.schedule(new JobPollTask(sessionId, jobId, initialPollInterval),
            initialPollInterval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          setResult(null, e);
          changeState(State.FAILED);
        }
      }
    };
    executor.submit(task);
  }

  private T get(boolean waitIndefinitely, long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (!isDone) {
      synchronized (lock) {
        if (waitIndefinitely) {
          while (!isDone) {
            lock.wait();
          }
        } else {
          long now = System.nanoTime();
          long deadline = now + unit.toNanos(timeout);
          while (!isDone && deadline > now) {
            lock.wait(TimeUnit.NANOSECONDS.toMillis(deadline - now));
            now = System.nanoTime();
          }
          if (!isDone) {
            throw new TimeoutException();
          }
        }
      }
    }
    if (error != null) {
      throw new ExecutionException(error);
    }
    return result;
  }

  private void setResult(T result, Throwable error) {
    synchronized (lock) {
      this.result = result;
      this.error = error;
      this.isDone = true;
      lock.notifyAll();
    }
  }

  private class JobPollTask implements Runnable {

    private final int sessionId;
    private final long jobId;
    private long currentInterval;

    JobPollTask(int sessionId, long jobId, long currentInterval) {
      this.sessionId = sessionId;
      this.jobId = jobId;
      this.currentInterval = currentInterval;
    }

    @Override
    public void run() {
      try {
        JobStatus status = conn.get(JobStatus.class, "/%d/jobs/%d", sessionId, jobId);
        T result = null;
        Throwable error = null;
        boolean finished = false;
        switch (status.state) {
          case SUCCEEDED:
            result = (T) serializer.deserialize(ByteBuffer.wrap(status.result));
            finished = true;
            break;

          case FAILED:
            // TODO: better exception.
            error = new RuntimeException(status.error);
            finished = true;
            break;

          case CANCELLED:
            isCancelled = true;
            finished = true;
            break;

          default:
            // Nothing to do.
        }
        if (finished) {
          setResult(result, error);
        }
        if (status.state != state) {
          changeState(status.state);
        }
        if (!finished) {
          currentInterval = Math.min(currentInterval * 2, maxPollInterval);
          pollTask = executor.schedule(this, currentInterval, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        setResult(null, e);
        changeState(State.FAILED);
      }
    }

  }

}
