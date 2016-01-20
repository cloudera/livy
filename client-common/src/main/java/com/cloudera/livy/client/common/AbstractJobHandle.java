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

package com.cloudera.livy.client.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.MetricsCollection;
import com.cloudera.livy.annotations.Private;

@Private
public abstract class AbstractJobHandle<T> implements JobHandle<T> {

  private final List<Integer> sparkJobIds;
  protected final List<Listener<T>> listeners;
  protected final MetricsCollection metrics;
  protected volatile State state;

  protected AbstractJobHandle() {
    this.listeners = new LinkedList<>();
    this.metrics = new MetricsCollection();
    this.sparkJobIds = new CopyOnWriteArrayList<Integer>();
    this.state = State.SENT;
  }

  /**
   * A collection of metrics collected from the Spark jobs triggered by this job.
   *
   * To collect job metrics on the client, Spark jobs must be registered with JobContext::monitor()
   * on the remote end.
   */
  @Override
  public MetricsCollection getMetrics() {
    return metrics;
  }

  @Override
  public List<Integer> getSparkJobIds() {
    return sparkJobIds;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addListener(Listener<T> l) {
    synchronized (listeners) {
      listeners.add(l);
      // If current state is a final state, notify of Spark job IDs before notifying about the
      // state transition.
      if (state.ordinal() >= State.CANCELLED.ordinal()) {
        for (Integer i : sparkJobIds) {
          l.onSparkJobStarted(this, i);
        }
      }

      fireStateChange(state, l);

      // Otherwise, notify about Spark jobs after the state notification.
      if (state.ordinal() < State.CANCELLED.ordinal()) {
        for (Integer i : sparkJobIds) {
          l.onSparkJobStarted(this, i);
        }
      }
    }
  }

  /**
   * Changes the state of this job handle, making sure that illegal state transitions are ignored.
   * Fires events appropriately.
   *
   * As a rule, state transitions can only occur if the current state is "higher" than the current
   * state (i.e., has a higher ordinal number) and is not a "final" state. "Final" states are
   * CANCELLED, FAILED and SUCCEEDED, defined here in the code as having an ordinal number higher
   * than the CANCELLED enum constant.
   */
  public boolean changeState(State newState) {
    synchronized (listeners) {
      if (newState.ordinal() > state.ordinal() && state.ordinal() < State.CANCELLED.ordinal()) {
        state = newState;
        for (Listener<T> l : listeners) {
          fireStateChange(newState, l);
        }
        return true;
      }
      return false;
    }
  }

  public void addSparkJobId(int sparkJobId) {
    synchronized (listeners) {
      sparkJobIds.add(sparkJobId);
      for (Listener<T> l : listeners) {
        l.onSparkJobStarted(this, sparkJobId);
      }
    }
  }

  protected abstract T result();
  protected abstract Throwable error();

  private void fireStateChange(State s, Listener<T> l) {
    switch (s) {
    case SENT:
      break;
    case QUEUED:
      l.onJobQueued(this);
      break;
    case STARTED:
      l.onJobStarted(this);
      break;
    case CANCELLED:
      l.onJobCancelled(this);
      break;
    case FAILED:
      l.onJobFailed(this, error());
      break;
    case SUCCEEDED:
      try {
        l.onJobSucceeded(this, result());
      } catch (Exception e) {
        // Shouldn't really happen.
        throw new IllegalStateException(e);
      }
      break;
    default:
      throw new IllegalStateException();
    }
  }

}
