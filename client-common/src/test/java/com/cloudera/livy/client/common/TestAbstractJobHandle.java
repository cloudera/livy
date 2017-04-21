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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.InOrder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.JobHandle.State;

public class TestAbstractJobHandle {

  @Test
  public void testJobHandle() {
    AbstractJobHandle<Void> handle = new TestJobHandle();

    assertTrue(handle.changeState(State.QUEUED));
    assertEquals(State.QUEUED, handle.getState());

    @SuppressWarnings("unchecked")
    JobHandle.Listener<Void> l1 = mock(JobHandle.Listener.class);
    handle.addListener(l1);
    verify(l1).onJobQueued(handle);

    assertTrue(handle.changeState(State.STARTED));
    verify(l1).onJobStarted(handle);

    assertTrue(handle.changeState(State.SUCCEEDED));
    verify(l1).onJobSucceeded(handle, null);

    assertFalse(handle.changeState(State.CANCELLED));
  }

  private static class TestJobHandle extends AbstractJobHandle<Void> {

    @Override
    protected Void result() {
      return null;
    }

    @Override
    protected Throwable error() {
      return null;
    }

    @Override
    public Void get() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void get(long l, TimeUnit t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean b) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public long getJobId() {
      throw new UnsupportedOperationException();
    }

  }

}
