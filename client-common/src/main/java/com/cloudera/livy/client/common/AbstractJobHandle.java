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

package com.cloudera.livy.client.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.annotations.Private;

@Private
public abstract class AbstractJobHandle<T> implements JobHandle<T> {

  protected final List<Listener<T>> listeners;
  protected volatile State state;

  protected AbstractJobHandle() {
    this.listeners = new LinkedList<>();
    this.state = State.SENT;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addListener(Listener<T> l) {
    synchronized (listeners) {
      listeners.add(l);
      fireStateChange(state, l);
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
