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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.client.common.AbstractJobHandle;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.common.Serializer;
import static com.cloudera.livy.client.common.HttpMessages.*;
import static com.cloudera.livy.client.http.HttpConf.Entry.*;

class JobHandleImpl<T> extends AbstractJobHandle<T> {

  private final long sessionId;
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
  private volatile boolean isCancelPending;
  private volatile ScheduledFuture<?> pollTask;

  JobHandleImpl(
      HttpConf config,
      LivyConnection conn,
      long sessionId,
      ScheduledExecutorService executor,
      Serializer s) {
    this.conn = conn;
    this.sessionId = sessionId;
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

    // The job ID is set asynchronously, and there might be a call to cancel() before it's
    // set. So cancel() will always set the isCancelPending flag, even if there's no job
    // ID yet. If the thread setting the job ID sees that flag, it will send a cancel request
    // to the server. There's still a possibility that two cancel requests will be sent,
    // but that doesn't cause any harm.
    this.isCancelPending = false;
    this.jobId = -1;
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
  public boolean cancel(final boolean mayInterrupt) {
    // Do a best-effort to detect if already cancelled, but the final say is always
    // on the server side. Don't block the caller, though.
    if (!isCancelled && !isCancelPending) {
      isCancelPending = true;
      if (jobId > -1) {
        sendCancelRequest(jobId);
      }
      return true;
    }

    return false;
  }

  @Override
  protected T result() {
    return result;
  }

  @Override
  protected Throwable error() {
    return error;
  }

  void start(final String command, final ByteBuffer serializedJob) {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          ClientMessage msg = new SerializedJob(BufferUtils.toByteArray(serializedJob));
          JobStatus status = conn.post(msg, JobStatus.class, "/%d/%s", sessionId, command);

          if (isCancelPending) {
            sendCancelRequest(status.id);
          }

          jobId = status.id;

          pollTask = executor.schedule(new JobPollTask(initialPollInterval),
            initialPollInterval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          setResult(null, e, State.FAILED);
        }
      }
    };
    executor.submit(task);
  }

  private void sendCancelRequest(final long id) {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          conn.post(null, Void.class, "/%d/jobs/%d/cancel", sessionId, id);
        } catch (Exception e) {
          setResult(null, e, State.FAILED);
        }
      }
    });
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
    if (isCancelled) {
      throw new CancellationException();
    }
    if (error != null) {
      throw new ExecutionException(error);
    }
    return result;
  }

  private void setResult(T result, Throwable error, State newState) {
    if (!isDone) {
      synchronized (lock) {
        if (!isDone) {
          this.result = result;
          this.error = error;
          this.isDone = true;
          changeState(newState);
        }
        lock.notifyAll();
      }
    }
  }

  private class JobPollTask implements Runnable {

    private long currentInterval;

    JobPollTask(long currentInterval) {
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
            if (status.result != null) {
              @SuppressWarnings("unchecked")
              T localResult = (T) serializer.deserialize(ByteBuffer.wrap(status.result));
              result = localResult;
            }
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
          setResult(result, error, status.state);
        } else if (status.state != state) {
          changeState(status.state);
        }
        if (!finished) {
          currentInterval = Math.min(currentInterval * 2, maxPollInterval);
          pollTask = executor.schedule(this, currentInterval, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        setResult(null, e, State.FAILED);
      }
    }

  }
  
  @Override
  public long getJobId() {
    return jobId;
  }

}
